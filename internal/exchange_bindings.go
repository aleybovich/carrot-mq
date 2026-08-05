package internal

import (
	"fmt"
	"slices"
)

// alternateExchangeArgKey is the exchange.declare argument naming the exchange that
// receives messages an exchange could not route itself.
const alternateExchangeArgKey = "alternate-exchange"

// exchangeBinding identifies a single exchange-to-exchange binding.
type exchangeBinding struct {
	Source      string
	Destination string
	RoutingKey  string
}

// alternateExchangeName returns the exchange named by the "alternate-exchange" argument,
// or "" when the argument is absent. A non-string value is rejected at declare time, so a
// stored arguments table always carries a string here.
func alternateExchangeName(args map[string]interface{}) string {
	name, _ := args[alternateExchangeArgKey].(string)
	return name
}

// validateAlternateExchange rejects a present-but-non-string "alternate-exchange"
// argument and otherwise returns the exchange name it carries.
func validateAlternateExchange(args map[string]interface{}) (string, error) {
	if raw, exists := args[alternateExchangeArgKey]; exists {
		if _, isString := raw.(string); !isString {
			return "", fmt.Errorf("invalid '%s' argument: expected a string, got %T", alternateExchangeArgKey, raw)
		}
	}
	return alternateExchangeName(args), nil
}

// matchTargets snapshots the queues and destination exchanges that ex binds for
// routingKey - applying ex's own exchange type to both binding tables - along with ex's
// alternate exchange. The lock is released before returning so that callers may recurse
// into other exchanges, including back into ex, without self-deadlocking on a cycle.
func (ex *exchange) matchTargets(routingKey string) (queues, exchanges []string, alternate string, err error) {
	ex.mu.RLock()
	defer ex.mu.RUnlock()

	var collect func(bindings map[string][]string) []string
	switch ex.Type {
	case "direct":
		collect = func(bindings map[string][]string) []string {
			return slices.Clone(bindings[routingKey])
		}
	case "fanout":
		collect = func(bindings map[string][]string) []string {
			var matched []string
			for _, names := range bindings {
				matched = append(matched, names...)
			}
			return matched
		}
	case "topic":
		collect = func(bindings map[string][]string) []string {
			var matched []string
			for pattern, names := range bindings {
				if topicMatch(pattern, routingKey) {
					matched = append(matched, names...)
				}
			}
			return matched
		}
	default:
		return nil, nil, "", fmt.Errorf("unknown exchange type: %s", ex.Type)
	}

	return collect(ex.Bindings), collect(ex.ExchangeBindings), alternateExchangeName(ex.Arguments), nil
}

// routeThroughExchange walks the binding graph from ex, appending every queue the message
// reaches to queues while skipping duplicates. An exchange is processed at most once per
// publish, which is what makes binding cycles and alternate-exchange cycles terminate.
func (c *connection) routeThroughExchange(vhost *vHost, ex *exchange, routingKey string, visited, seenQueues map[string]bool, queues *[]string) error {
	if visited[ex.Name] {
		return nil
	}
	visited[ex.Name] = true

	matchedQueues, matchedExchanges, alternate, err := ex.matchTargets(routingKey)
	if err != nil {
		return err
	}

	for _, queueName := range matchedQueues {
		if !seenQueues[queueName] {
			seenQueues[queueName] = true
			*queues = append(*queues, queueName)
		}
	}

	// Every hop matches against the message's original routing key.
	for _, destinationName := range matchedExchanges {
		destination := vhost.lookupExchange(destinationName)
		if destination == nil {
			c.server.Warn("Exchange '%s' is bound to missing exchange '%s'; skipping", ex.Name, destinationName)
			continue
		}
		if err := c.routeThroughExchange(vhost, destination, routingKey, visited, seenQueues, queues); err != nil {
			return err
		}
	}

	// The alternate exchange is consulted only when this exchange's own bindings - queue
	// bindings and exchange bindings together - matched nothing at all.
	if len(matchedQueues) > 0 || len(matchedExchanges) > 0 || alternate == "" {
		return nil
	}

	alternateEx := vhost.lookupExchange(alternate)
	if alternateEx == nil {
		c.server.Warn("Alternate exchange '%s' of exchange '%s' does not exist; message stays unroutable", alternate, ex.Name)
		return nil
	}
	c.server.Debug("Forwarding message from exchange '%s' to alternate exchange '%s'", ex.Name, alternate)
	return c.routeThroughExchange(vhost, alternateEx, routingKey, visited, seenQueues, queues)
}

// addExchangeBinding records ex -> destination under routingKey. It reports false when
// the binding already existed, making a repeated exchange.bind an idempotent no-op.
func (ex *exchange) addExchangeBinding(routingKey, destination string) bool {
	ex.mu.Lock()
	defer ex.mu.Unlock()

	if slices.Contains(ex.ExchangeBindings[routingKey], destination) {
		return false
	}
	ex.ExchangeBindings[routingKey] = append(ex.ExchangeBindings[routingKey], destination)
	return true
}

// removeExchangeBinding drops ex -> destination under routingKey, reporting whether a
// binding was actually removed.
func (ex *exchange) removeExchangeBinding(routingKey, destination string) bool {
	ex.mu.Lock()
	defer ex.mu.Unlock()

	return removeBoundName(ex.ExchangeBindings, routingKey, destination)
}

// removeExchangeBindingsTo drops every binding from ex to destination, whatever the
// routing key, and returns the routing keys whose bindings were removed.
func (ex *exchange) removeExchangeBindingsTo(destination string) []string {
	ex.mu.Lock()
	defer ex.mu.Unlock()

	var removed []string
	for routingKey := range ex.ExchangeBindings {
		if removeBoundName(ex.ExchangeBindings, routingKey, destination) {
			removed = append(removed, routingKey)
		}
	}
	return removed
}

// takeExchangeBindings removes and returns every exchange binding for which ex is the
// source. Used when ex itself is being deleted.
func (ex *exchange) takeExchangeBindings() []exchangeBinding {
	ex.mu.Lock()
	defer ex.mu.Unlock()

	removed := make([]exchangeBinding, 0, len(ex.ExchangeBindings))
	for routingKey, destinations := range ex.ExchangeBindings {
		for _, destination := range destinations {
			removed = append(removed, exchangeBinding{Source: ex.Name, Destination: destination, RoutingKey: routingKey})
		}
	}
	ex.ExchangeBindings = make(map[string][]string)
	return removed
}

// hasOutboundBindings reports whether ex is the source of any binding, to a queue or to
// another exchange. That is the "in use" condition for exchange.delete with if-unused.
func (ex *exchange) hasOutboundBindings() bool {
	ex.mu.RLock()
	defer ex.mu.RUnlock()

	return len(ex.Bindings) > 0 || len(ex.ExchangeBindings) > 0
}

// removeBoundName removes name from bindings[routingKey], pruning the entry once it is
// empty so that "has any binding" checks stay accurate. Callers must hold the lock.
func removeBoundName(bindings map[string][]string, routingKey, name string) bool {
	current, exists := bindings[routingKey]
	if !exists {
		return false
	}

	remaining := slices.DeleteFunc(current, func(bound string) bool { return bound == name })
	if len(remaining) == len(current) {
		return false
	}
	if len(remaining) == 0 {
		delete(bindings, routingKey)
	} else {
		bindings[routingKey] = remaining
	}
	return true
}

// cleanupExchangeBindingsForExchange removes every exchange-to-exchange binding in which
// exchangeName takes part - as source or as destination - and returns them so the caller
// can drop the matching persistence records. Inbound edges are removed silently; they
// never block deletion.
func (c *connection) cleanupExchangeBindingsForExchange(vhost *vHost, exchangeName string) []exchangeBinding {
	vhost.mu.RLock()
	exchanges := make([]*exchange, 0, len(vhost.exchanges))
	for _, ex := range vhost.exchanges {
		exchanges = append(exchanges, ex)
	}
	vhost.mu.RUnlock()

	removed := make([]exchangeBinding, 0)
	for _, ex := range exchanges {
		if ex.Name == exchangeName {
			removed = append(removed, ex.takeExchangeBindings()...)
			continue
		}
		for _, routingKey := range ex.removeExchangeBindingsTo(exchangeName) {
			removed = append(removed, exchangeBinding{Source: ex.Name, Destination: exchangeName, RoutingKey: routingKey})
		}
	}

	if len(removed) > 0 {
		c.server.Debug("Removed %d exchange binding(s) involving exchange '%s'", len(removed), exchangeName)
	}
	return removed
}
