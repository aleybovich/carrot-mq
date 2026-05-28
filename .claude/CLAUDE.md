# CLAUDE.md — cph2.api

After reading this file, clearly state that you have read it without being prompted.

## Part A: Behavioral Guidelines

**Tradeoff:** These guidelines bias toward caution over speed. For trivial tasks, use judgment.

Always apply DRY and YAGNI first, use SOLID only when it clearly improves maintainability, and prioritize readability over cleverness.

### 0. Instruction Order

When guidance conflicts, use this order:
- User request, if safe and unambiguous
- Platform and safety constraints
- Repo architecture and correctness rules
- Style and workflow preferences

### 1. Think Before Coding

**Don't assume. Don't hide confusion. Surface tradeoffs.**

Before implementing:
- State assumptions only when they materially affect the solution.
- If multiple interpretations would change behavior, present them — don't pick silently.
- If a simpler approach exists, say so. Push back when warranted.
- Ask questions when blocked, when the change is risky, or when ambiguity would change behavior.

### 2. Simplicity First

**Minimum code that solves the problem. Nothing speculative.**

- No features beyond what was asked.
- No abstractions for single-use code.
- No "flexibility" or "configurability" that wasn't requested.
- If you write 200 lines and it could be 50, rewrite it.

Ask yourself: "Would a senior engineer say this is overcomplicated?" If yes, simplify.

### 3. Surgical Changes

**Touch only what you must. Clean up only your own mess.**

When editing existing code:
- Don't "improve" adjacent code, comments, or formatting.
- Don't refactor things that aren't broken.
- Match existing style, even if you'd do it differently.
- If you notice unrelated dead code, mention it — don't delete it.

When your changes create orphans:
- Remove imports/variables/functions that YOUR changes made unused.
- Don't remove pre-existing dead code unless asked.

The test: Every changed line should trace directly to the user's request.

### 4. Verify Before Generate

**Read the source. Do not infer from convention.**

Before calling any function, using any type, or placing a file: search the codebase for the actual definition and read it. Do not guess signatures, package locations, or directory structure. If you can't find it, say so — don't invent a plausible substitute.

### 5. Goal-Driven Execution

**Define success criteria. Loop until verified.**

Transform tasks into verifiable goals:
- "Add validation" -> "Choose the smallest reliable verification path, then make it pass"
- "Fix the bug" -> "Prefer a failing test when practical, then make it pass"
- "Refactor X" -> "Ensure tests or equivalent checks pass before and after"

For multi-step tasks, state a brief plan:
```
1. [Step] -> verify: [check]
2. [Step] -> verify: [check]
```

---

## Part B: Go Conventions (Where We Deviate or Emphasize)

Target the current Go version (check with `go version`). Use all latest stable language features and modernisation expectations (e.g., use `any` instead of `interface{}`, `reflect.TypeFor[T]()` instead of `reflect.TypeOf((*T)(nil)).Elem()`, etc.)

**Project-specific deviations from standard Go:**
- Actor package names use underscores (e.g., `order_processor`). Follow existing convention.
- Use `github.com/stretchr/testify/assert` for all tests. Do not introduce other test frameworks.
- Use generic helpers from `components/generics` (`Filter`, `ArrayTransform`, `Ptr`, `Deref`, `MapFromArray`, etc.) — don't reimplement them.

**Emphasis:**
- Handle every error. Never discard an `error` return without explicit justification.
- Wrap errors with context: `fmt.Errorf("doing X: %w", err)`.
- `context.Context` is always the first parameter for request-scoped, blocking, or cancelable work. Never store contexts in structs. Never pass nil context.
- Close response bodies, files, rows, and other acquired resources on all exit paths. Stop timers and tickers when no longer needed.
- Never leak goroutines — always wire cancellation. Bound concurrency intentionally.
- Return errors from library/request-path code; reserve `panic` for truly unrecoverable startup invariants.
- After adding/removing deps: `go mod tidy`.
- Arithmetic on Value structs (`Value float64` + `Unit string`) must use `github.com/cme-eventhub/components/units` functions (`units.Sub`, `units.Sum`, etc.) — never raw float arithmetic. Values may carry different units and require conversion.

### Goroutine Ownership
- For goroutines started by actors, subscriptions, or consumers: document the owner and shutdown path.
- `Stop()` must stop tickers/timers and allow workers to exit cleanly.
