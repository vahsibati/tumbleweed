package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"

	"tumbleweed/pkg/client"
	"tumbleweed/pkg/protocol"
)

func main() {
	// Parse global flags
	fs := flag.NewFlagSet("tumbleweed-cli", flag.ExitOnError)
	addr := fs.String("addr", ":8765", "Tumbleweed broker address")

	// Custom Usage output
	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [global flags] <command> [command flags/arguments]\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Global Flags:\n")
		fs.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nCommands:\n")
		fmt.Fprintf(os.Stderr, "  ping\n")
		fmt.Fprintf(os.Stderr, "        Ping the Tumbleweed broker and measure latency\n")
		fmt.Fprintf(os.Stderr, "  create-topic <topic>\n")
		fmt.Fprintf(os.Stderr, "        Create a new topic\n")
		fmt.Fprintf(os.Stderr, "  produce <topic> <message>\n")
		fmt.Fprintf(os.Stderr, "        Produce a message to the specified topic\n")
		fmt.Fprintf(os.Stderr, "        Flags:\n")
		fmt.Fprintf(os.Stderr, "          -key string  Optional message key\n")
		fmt.Fprintf(os.Stderr, "  fetch <topic> <group>\n")
		fmt.Fprintf(os.Stderr, "        Fetch messages from the topic using a consumer group\n")
		fmt.Fprintf(os.Stderr, "        Flags:\n")
		fmt.Fprintf(os.Stderr, "          -consumer string  Consumer ID (default is auto-generated)\n")
		fmt.Fprintf(os.Stderr, "          -max int          Maximum number of messages to fetch (default: 1)\n")
		fmt.Fprintf(os.Stderr, "          -ack-timeout ms   Visibility timeout in milliseconds (default: 30000)\n")
		fmt.Fprintf(os.Stderr, "          -poll-timeout ms  Long-polling timeout in milliseconds (default: 0)\n")
		fmt.Fprintf(os.Stderr, "  ack <topic> <group> <offset>\n")
		fmt.Fprintf(os.Stderr, "        Acknowledge message processing for an offset\n")
		fmt.Fprintf(os.Stderr, "  nack <topic> <group> <offset>\n")
		fmt.Fprintf(os.Stderr, "        Negative-acknowledge message processing for an offset\n")
		fmt.Fprintf(os.Stderr, "  list-topics (aliases: list, topics)\n")
		fmt.Fprintf(os.Stderr, "        List all active topics in the broker\n")
	}

	// Rearrange arguments to support global flags specified after the subcommand
	rearranged := rearrangeArgs(os.Args[1:])

	if len(rearranged) == 0 {
		fs.Usage()
		os.Exit(0)
	}

	if err := fs.Parse(rearranged); err != nil {
		fmt.Fprintf(os.Stderr, "Error parsing flags: %v\n", err)
		os.Exit(1)
	}

	args := fs.Args()
	if len(args) < 1 {
		fs.Usage()
		os.Exit(0)
	}

	cmd := args[0]
	cmdArgs := args[1:]

	// Create and connect client
	cli := client.NewClient(*addr)
	if err := cli.Connect(); err != nil {
		fmt.Fprintf(os.Stderr, "Error connecting to broker at %s: %v\n", *addr, err)
		os.Exit(1)
	}
	defer cli.Close()

	switch cmd {
	case "ping":
		latency, err := cli.Ping()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Ping failed: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("Pong! Latency: %s\n", latency)

	case "create-topic":
		if len(cmdArgs) < 1 {
			fmt.Fprintf(os.Stderr, "Usage: tumbleweed-cli create-topic <topic>\n")
			os.Exit(1)
		}
		topic := cmdArgs[0]
		if err := cli.CreateTopic(topic); err != nil {
			fmt.Fprintf(os.Stderr, "Error creating topic '%s': %v\n", topic, err)
			os.Exit(1)
		}
		fmt.Printf("Topic '%s' created successfully.\n", topic)

	case "produce":
		prodFs := flag.NewFlagSet("produce", flag.ExitOnError)
		keyFlag := prodFs.String("key", "", "Optional message key")
		_ = prodFs.Parse(cmdArgs)

		pArgs := prodFs.Args()
		if len(pArgs) < 2 {
			fmt.Fprintf(os.Stderr, "Usage: tumbleweed-cli produce [flags] <topic> <message>\n")
			prodFs.PrintDefaults()
			os.Exit(1)
		}
		topic := pArgs[0]
		msg := pArgs[1]

		var key []byte
		if *keyFlag != "" {
			key = []byte(*keyFlag)
		}

		offset, err := cli.Produce(topic, key, []byte(msg))
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error producing message: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("Message produced successfully! Offset: %d\n", offset)

	case "fetch":
		fetchFs := flag.NewFlagSet("fetch", flag.ExitOnError)
		consumerFlag := fetchFs.String("consumer", "", "Consumer ID (default auto-generated)")
		maxFlag := fetchFs.Int("max", 1, "Maximum number of messages to fetch")
		ackTimeoutFlag := fetchFs.Int("ack-timeout", 30000, "Ack timeout in milliseconds")
		pollTimeoutFlag := fetchFs.Int("poll-timeout", 0, "Long poll timeout in milliseconds")
		_ = fetchFs.Parse(cmdArgs)

		fArgs := fetchFs.Args()
		if len(fArgs) < 2 {
			fmt.Fprintf(os.Stderr, "Usage: tumbleweed-cli fetch [flags] <topic> <group>\n")
			fetchFs.PrintDefaults()
			os.Exit(1)
		}
		topic := fArgs[0]
		group := fArgs[1]

		consumerID := *consumerFlag
		if consumerID == "" {
			// standard pseudo-random number generator
			consumerID = fmt.Sprintf("cli-consumer-%d", rand.Intn(100000))
		}

		req := &protocol.FetchRequest{
			Topic:             topic,
			Group:             group,
			ConsumerID:        consumerID,
			MaxMessages:       uint32(*maxFlag),
			AckTimeoutMs:      uint32(*ackTimeoutFlag),
			LongPollTimeoutMs: uint32(*pollTimeoutFlag),
		}

		messages, err := cli.Fetch(req)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error fetching messages: %v\n", err)
			os.Exit(1)
		}

		if len(messages) == 0 {
			fmt.Println("No messages found.")
			return
		}

		fmt.Printf("Fetched %d message(s) (Consumer ID: %s):\n\n", len(messages), consumerID)
		for _, m := range messages {
			fmt.Printf("--- Message %d ---\n", m.Offset)
			fmt.Printf("  Offset:    %d\n", m.Offset)
			fmt.Printf("  Timestamp: %s\n", time.Unix(0, m.Timestamp).Format(time.RFC3339Nano))
			if len(m.Key) > 0 {
				fmt.Printf("  Key:       %s\n", string(m.Key))
			} else {
				fmt.Printf("  Key:       <empty>\n")
			}
			fmt.Printf("  Value:     %s\n", string(m.Value))
			fmt.Println()
		}

	case "ack":
		if len(cmdArgs) < 3 {
			fmt.Fprintf(os.Stderr, "Usage: tumbleweed-cli ack <topic> <group> <offset>\n")
			os.Exit(1)
		}
		topic := cmdArgs[0]
		group := cmdArgs[1]
		offsetVal, err := strconv.ParseUint(cmdArgs[2], 10, 64)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Invalid offset value: %v\n", err)
			os.Exit(1)
		}

		if err := cli.Ack(topic, group, offsetVal); err != nil {
			fmt.Fprintf(os.Stderr, "Error sending ACK: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("ACK sent successfully for offset %d in group '%s' (topic '%s').\n", offsetVal, group, topic)

	case "nack":
		if len(cmdArgs) < 3 {
			fmt.Fprintf(os.Stderr, "Usage: tumbleweed-cli nack <topic> <group> <offset>\n")
			os.Exit(1)
		}
		topic := cmdArgs[0]
		group := cmdArgs[1]
		offsetVal, err := strconv.ParseUint(cmdArgs[2], 10, 64)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Invalid offset value: %v\n", err)
			os.Exit(1)
		}

		if err := cli.Nack(topic, group, offsetVal); err != nil {
			fmt.Fprintf(os.Stderr, "Error sending NACK: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("NACK sent successfully for offset %d in group '%s' (topic '%s').\n", offsetVal, group, topic)

	case "list-topics", "list", "topics":
		topics, err := cli.ListTopics()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error listing topics: %v\n", err)
			os.Exit(1)
		}
		if len(topics) == 0 {
			fmt.Println("No topics found.")
			return
		}
		fmt.Println("Topics:")
		for _, t := range topics {
			fmt.Printf("  - %s\n", t)
		}

	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n\n", cmd)
		fs.Usage()
		os.Exit(1)
	}
}

// rearrangeArgs moves global flags (-addr, --addr) and their values to the front of args
func rearrangeArgs(args []string) []string {
	var globalFlags []string
	var otherArgs []string

	i := 0
	for i < len(args) {
		arg := args[i]
		if arg == "-addr" || arg == "--addr" {
			if i+1 < len(args) {
				globalFlags = append(globalFlags, arg, args[i+1])
				i += 2
			} else {
				globalFlags = append(globalFlags, arg)
				i++
			}
		} else if strings.HasPrefix(arg, "-addr=") || strings.HasPrefix(arg, "--addr=") {
			globalFlags = append(globalFlags, arg)
			i++
		} else {
			otherArgs = append(otherArgs, arg)
			i++
		}
	}

	return append(globalFlags, otherArgs...)
}
