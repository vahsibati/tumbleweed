package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"tumbleweed/pkg/client"
	"tumbleweed/pkg/protocol"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	subcommand := os.Args[1]

	addr := "localhost:8765"

	switch subcommand {
	case "create-topic":
		cmd := flag.NewFlagSet("create-topic", flag.ExitOnError)
		topic := cmd.String("topic", "", "name of the topic to create")
		cmd.StringVar(&addr, "addr", "localhost:8765", "broker server address")
		cmd.Parse(os.Args[2:])

		if *topic == "" {
			log.Fatal("Error: --topic is required")
		}

		cli, err := client.NewClient(addr)
		if err != nil {
			log.Fatalf("Failed to connect to broker: %v", err)
		}
		defer cli.Close()

		if err := cli.CreateTopic(*topic); err != nil {
			log.Fatalf("Failed to create topic: %v", err)
		}
		fmt.Printf("Topic '%s' created successfully.\n", *topic)

	case "produce":
		cmd := flag.NewFlagSet("produce", flag.ExitOnError)
		topic := cmd.String("topic", "", "topic to publish to")
		key := cmd.String("key", "", "message key")
		val := cmd.String("value", "", "message value")
		cmd.StringVar(&addr, "addr", "localhost:8765", "broker server address")
		cmd.Parse(os.Args[2:])

		if *topic == "" || *val == "" {
			log.Fatal("Error: --topic and --value are required")
		}

		cli, err := client.NewClient(addr)
		if err != nil {
			log.Fatalf("Failed to connect to broker: %v", err)
		}
		defer cli.Close()

		offset, err := cli.Publish(*topic, []byte(*key), []byte(*val))
		if err != nil {
			log.Fatalf("Failed to publish message: %v", err)
		}
		fmt.Printf("Published message to topic '%s' at offset %d\n", *topic, offset)

	case "consume":
		cmd := flag.NewFlagSet("consume", flag.ExitOnError)
		topic := cmd.String("topic", "", "topic to consume from")
		group := cmd.String("group", "", "consumer group name")
		consID := cmd.String("consumer", "", "consumer ID (optional)")
		cmd.StringVar(&addr, "addr", "localhost:8765", "broker server address")
		cmd.Parse(os.Args[2:])

		if *topic == "" || *group == "" {
			log.Fatal("Error: --topic and --group are required")
		}

		consumerID := *consID
		if consumerID == "" {
			r := rand.New(rand.NewSource(time.Now().UnixNano()))
			consumerID = fmt.Sprintf("consumer-%d", r.Intn(100000))
		}

		cli, err := client.NewClient(addr)
		if err != nil {
			log.Fatalf("Failed to connect to broker: %v", err)
		}
		defer cli.Close()

		log.Printf("Starting consumer '%s' in group '%s' for topic '%s'...", consumerID, *group, *topic)

		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			<-sigChan
			log.Println("Stopping consumer...")
			cancel()
		}()

		err = cli.Subscribe(ctx, *topic, *group, consumerID, func(msg protocol.Message) error {
			fmt.Printf(" [x] Received Offset %d | Key: %q | Value: %s\n", msg.Offset, string(msg.Key), string(msg.Value))
			return nil
		})

		if err != nil && err != context.Canceled {
			log.Fatalf("Consumer error: %v", err)
		}

	default:
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println("Tumbleweed CLI Tool")
	fmt.Println("Usage:")
	fmt.Println("  tumbleweed-cli <subcommand> [options]")
	fmt.Println()
	fmt.Println("Subcommands:")
	fmt.Println("  create-topic   Creates a new topic on the broker")
	fmt.Println("  produce        Publishes a message to a topic")
	fmt.Println("  consume        Subscribes to a topic under a consumer group")
	fmt.Println()
	fmt.Println("Run 'tumbleweed-cli <subcommand> --help' to see subcommand options.")
}
