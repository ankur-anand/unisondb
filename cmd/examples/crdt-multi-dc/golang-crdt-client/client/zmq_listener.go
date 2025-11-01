package client

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/ankur-anand/unisondb/examples/golang-crdt-client/crdt"
	zmq "github.com/pebbe/zmq4"
)

// ZMQListener listens to change notifications from UnisonDB relayer via ZeroMQ.
type ZMQListener struct {
	// namespace -> zmq address
	namespaces   map[string]string
	stateManager *crdt.StateManager
	httpClient   *HTTPClient
	sockets      []*zmq.Socket
	mu           sync.Mutex
}

// NewZMQListener creates a new ZeroMQ listener.
func NewZMQListener(namespaces map[string]string, stateManager *crdt.StateManager, httpClient *HTTPClient) *ZMQListener {
	return &ZMQListener{
		namespaces:   namespaces,
		stateManager: stateManager,
		httpClient:   httpClient,
		sockets:      make([]*zmq.Socket, 0),
	}
}

// ChangeNotification represents a change notification from UnisonDB.
type ChangeNotification struct {
	Key       string `json:"key"`
	Namespace string `json:"namespace"`
	Operation string `json:"operation"`
	EntryType string `json:"entry_type"`
	SeqNum    int64  `json:"seq_num"`
}

func (l *ZMQListener) Start(ctx context.Context) error {
	var wg sync.WaitGroup

	for namespace, zmqAddr := range l.namespaces {
		wg.Add(1)
		go func(ns, addr string) {
			defer wg.Done()
			if err := l.listenToNamespace(ctx, ns, addr); err != nil {
				fmt.Printf("Error listening to %s: %v\n", ns, err)
			}
		}(namespace, zmqAddr)
	}

	wg.Wait()
	return nil
}

func (l *ZMQListener) listenToNamespace(ctx context.Context, namespace, zmqAddr string) error {
	subscriber, err := zmq.NewSocket(zmq.SUB)
	if err != nil {
		return fmt.Errorf("failed to create ZeroMQ socket: %w", err)
	}

	l.mu.Lock()
	l.sockets = append(l.sockets, subscriber)
	l.mu.Unlock()

	defer subscriber.Close()

	fmt.Printf("Connecting to ZeroMQ %s: %s\n", namespace, zmqAddr)
	if err := subscriber.Connect(zmqAddr); err != nil {
		return fmt.Errorf("failed to connect to ZeroMQ: %w", err)
	}

	if err := subscriber.SetSubscribe(namespace); err != nil {
		return fmt.Errorf("failed to subscribe to %s: %w", namespace, err)
	}

	fmt.Printf("ZeroMQ listener started for namespace: %s\n", namespace)

	for {
		select {
		case <-ctx.Done():
			fmt.Printf("Stopping listener for namespace: %s\n", namespace)
			return nil
		default:
			topic, err := subscriber.Recv(0)
			if err != nil {
				fmt.Printf("Error receiving topic from %s: %v\n", namespace, err)
				continue
			}

			message, err := subscriber.Recv(0)
			if err != nil {
				fmt.Printf("Error receiving message from %s: %v\n", namespace, err)
				continue
			}

			if err := l.handleNotification(topic, message, namespace); err != nil {
				fmt.Printf("Error handling notification from %s: %v\n", namespace, err)
			}
		}
	}
}

func (l *ZMQListener) handleNotification(topic, message, namespace string) error {
	var notification ChangeNotification
	if err := json.Unmarshal([]byte(message), &notification); err != nil {
		return fmt.Errorf("failed to unmarshal notification: %w", err)
	}

	fmt.Printf("\nChange notification received\n")
	fmt.Printf("   Topic: %s\n", topic)
	fmt.Printf("   Namespace: %s\n", notification.Namespace)
	fmt.Printf("   Key: %s\n", notification.Key)
	fmt.Printf("   Operation: %s\n", notification.Operation)
	fmt.Printf("   Entry Type: %s\n", notification.EntryType)
	fmt.Printf("   Seq Num: %d\n", notification.SeqNum)

	if notification.Operation == "put" && notification.EntryType == "kv" {
		if err := l.httpClient.FetchValue(notification.Key, notification.Namespace); err != nil {
			fmt.Printf("Error fetching value for %s: %v\n", notification.Key, err)
		} else {
			l.stateManager.PrintState()
		}
	} else if notification.Operation == "delete" {
		fmt.Printf("Skipping delete operation\n")
	}

	return nil
}

func (l *ZMQListener) Stop() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	for _, socket := range l.sockets {
		socket.Close()
	}

	fmt.Println("ZeroMQ listeners stopped")
	return nil
}
