// server_test.go
package main

import (
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestServerConnection(t *testing.T) {
	server := NewServer()
	go server.Start(":5672")
	time.Sleep(100 * time.Millisecond)

	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("Failed to open channel: %v", err)
	}
	defer ch.Close()
}

func TestServerExchangeDeclare(t *testing.T) {
	server := NewServer()
	go server.Start(":5673")
	time.Sleep(100 * time.Millisecond)

	conn, err := amqp.Dial("amqp://guest:guest@localhost:5673/")
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("Failed to open channel: %v", err)
	}
	defer ch.Close()

	err = ch.ExchangeDeclare(
		"test-exchange",
		"direct",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		t.Fatalf("Failed to declare exchange: %v", err)
	}
}

func TestServerQueueDeclare(t *testing.T) {
	server := NewServer()
	go server.Start(":5674")
	time.Sleep(100 * time.Millisecond)

	conn, err := amqp.Dial("amqp://guest:guest@localhost:5674/")
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("Failed to open channel: %v", err)
	}
	defer ch.Close()

	_, err = ch.QueueDeclare(
		"test-queue",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		t.Fatalf("Failed to declare queue: %v", err)
	}
}

func TestServerPublishConsume(t *testing.T) {
	server := NewServer()
	go server.Start(":5675")
	time.Sleep(100 * time.Millisecond)

	conn, err := amqp.Dial("amqp://guest:guest@localhost:5675/")
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("Failed to open channel: %v", err)
	}
	defer ch.Close()

	err = ch.ExchangeDeclare(
		"test-exchange",
		"direct",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		t.Fatalf("Failed to declare exchange: %v", err)
	}

	q, err := ch.QueueDeclare(
		"test-queue",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		t.Fatalf("Failed to declare queue: %v", err)
	}

	err = ch.QueueBind(
		q.Name,
		"test-key",
		"test-exchange",
		false,
		nil,
	)
	if err != nil {
		t.Fatalf("Failed to bind queue: %v", err)
	}

	msgs, err := ch.Consume(
		q.Name,
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		t.Fatalf("Failed to consume: %v", err)
	}

	body := "Hello, World!"
	err = ch.Publish(
		"test-exchange",
		"test-key",
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(body),
		},
	)
	if err != nil {
		t.Fatalf("Failed to publish: %v", err)
	}

	select {
	case msg := <-msgs:
		if string(msg.Body) != body {
			t.Fatalf("Expected %s, got %s", body, string(msg.Body))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for message")
	}
}

func TestServerDirectRouting(t *testing.T) {
	server := NewServer()
	go server.Start(":5676")
	time.Sleep(100 * time.Millisecond)

	conn, err := amqp.Dial("amqp://guest:guest@localhost:5676/")
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("Failed to open channel: %v", err)
	}
	defer ch.Close()

	q1, err := ch.QueueDeclare("queue1", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("Failed to declare queue1: %v", err)
	}

	q2, err := ch.QueueDeclare("queue2", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("Failed to declare queue2: %v", err)
	}

	err = ch.QueueBind(q1.Name, "key1", "", false, nil)
	if err != nil {
		t.Fatalf("Failed to bind queue1: %v", err)
	}

	err = ch.QueueBind(q2.Name, "key2", "", false, nil)
	if err != nil {
		t.Fatalf("Failed to bind queue2: %v", err)
	}

	msgs1, err := ch.Consume(q1.Name, "", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("Failed to consume from queue1: %v", err)
	}

	msgs2, err := ch.Consume(q2.Name, "", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("Failed to consume from queue2: %v", err)
	}

	err = ch.Publish("", "key1", false, false, amqp.Publishing{
		Body: []byte("Message for queue1"),
	})
	if err != nil {
		t.Fatalf("Failed to publish to key1: %v", err)
	}

	err = ch.Publish("", "key2", false, false, amqp.Publishing{
		Body: []byte("Message for queue2"),
	})
	if err != nil {
		t.Fatalf("Failed to publish to key2: %v", err)
	}

	select {
	case msg := <-msgs1:
		if string(msg.Body) != "Message for queue1" {
			t.Fatalf("Unexpected message in queue1: %s", string(msg.Body))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for message in queue1")
	}

	select {
	case msg := <-msgs2:
		if string(msg.Body) != "Message for queue2" {
			t.Fatalf("Unexpected message in queue2: %s", string(msg.Body))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for message in queue2")
	}
}
