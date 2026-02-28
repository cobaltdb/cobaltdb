package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"strings"

	"github.com/cobaltdb/cobaltdb/pkg/wire"
)

func main() {
	var serverAddr = "localhost:4200"
	if len(os.Args) > 1 {
		serverAddr = os.Args[1]
	}

	fmt.Println("CobaltDB CLI")
	fmt.Printf("Connecting to %s...\n", serverAddr)

	conn, err := net.Dial("tcp", serverAddr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to connect: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	fmt.Println("Connected. Type 'exit' or 'quit' to exit.")
	fmt.Println()

	reader := bufio.NewReader(os.Stdin)

	for {
		fmt.Print("cobalt> ")

		line, err := reader.ReadString('\n')
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error reading input: %v\n", err)
			continue
		}

		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		if line == "exit" || line == "quit" {
			fmt.Println("Goodbye!")
			break
		}

		// Send query
		if err := sendQuery(conn, line); err != nil {
			fmt.Fprintf(os.Stderr, "Error sending query: %v\n", err)
			continue
		}

		// Read response
		response, err := readResponse(conn)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error reading response: %v\n", err)
			continue
		}

		printResponse(response)
	}
}

func sendQuery(conn net.Conn, sql string) error {
	query := wire.NewQueryMessage(sql)
	payload, err := wire.Encode(query)
	if err != nil {
		return err
	}

	// Write length
	length := uint32(1 + len(payload))
	if err := binary.Write(conn, binary.LittleEndian, length); err != nil {
		return err
	}

	// Write message type
	if err := binary.Write(conn, binary.LittleEndian, wire.MsgQuery); err != nil {
		return err
	}

	// Write payload
	if _, err := conn.Write(payload); err != nil {
		return err
	}

	return nil
}

func readResponse(conn net.Conn) (interface{}, error) {
	// Read length
	var length uint32
	if err := binary.Read(conn, binary.LittleEndian, &length); err != nil {
		return nil, err
	}

	// Read message type
	var msgType wire.MsgType
	if err := binary.Read(conn, binary.LittleEndian, &msgType); err != nil {
		return nil, err
	}

	// Read payload
	payload := make([]byte, length-1)
	if _, err := io.ReadFull(conn, payload); err != nil {
		return nil, err
	}

	switch msgType {
	case wire.MsgResult:
		var result wire.ResultMessage
		if err := wire.Decode(payload, &result); err != nil {
			return nil, err
		}
		return &result, nil

	case wire.MsgOK:
		var ok wire.OKMessage
		if err := wire.Decode(payload, &ok); err != nil {
			return nil, err
		}
		return &ok, nil

	case wire.MsgError:
		var errMsg wire.ErrorMessage
		if err := wire.Decode(payload, &errMsg); err != nil {
			return nil, err
		}
		return &errMsg, nil

	default:
		return nil, fmt.Errorf("unknown message type: %d", msgType)
	}
}

func printResponse(response interface{}) {
	switch r := response.(type) {
	case *wire.ResultMessage:
		if len(r.Columns) > 0 {
			// Print header
			for i, col := range r.Columns {
				if i > 0 {
					fmt.Print("\t")
				}
				fmt.Print(col)
			}
			fmt.Println()

			// Print rows
			for _, row := range r.Rows {
				for i, val := range row {
					if i > 0 {
						fmt.Print("\t")
					}
					fmt.Print(val)
				}
				fmt.Println()
			}

			fmt.Printf("(%d rows)\n", r.Count)
		}

	case *wire.OKMessage:
		if r.RowsAffected > 0 {
			fmt.Printf("Rows affected: %d\n", r.RowsAffected)
		}
		if r.LastInsertID > 0 {
			fmt.Printf("Last insert ID: %d\n", r.LastInsertID)
		}
		fmt.Println("OK")

	case *wire.ErrorMessage:
		fmt.Printf("Error: %s (code: %d)\n", r.Message, r.Code)
	}
}
