package main

import (
	"encoding/binary"
	"fmt"
	"net"
	"os"
)

// main serves as the program entry point
func main() {

	listener, err := net.Listen("tcp", "0.0.0.0:40000")
	if err != nil {
		fmt.Println("failed to create listener, err:", err)
		os.Exit(1)
	}
	fmt.Printf("listening on %s\n", listener.Addr())

	// listen for new connections
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("failed to accept connection, err:", err)
			continue
		}
		// pass an accepted connection to a handler goroutine
		go handleConnection(conn)
	}
}

// handleConnection handles the lifetime of a connection
func handleConnection(conn net.Conn) {
	defer conn.Close()
	println("New connection")
	data := make(map[int32]int32)
	for {
		// read client request data
		var msgType byte
		binary.Read(conn, binary.BigEndian, &msgType)
		//fmt.Printf("Message type: %d", msgType)
		if msgType == 'I' {
			var timestamp, price int32
			binary.Read(conn, binary.BigEndian, &timestamp)
			binary.Read(conn, binary.BigEndian, &price)
			fmt.Printf("Insert: %d %d\n", timestamp, price)
			data[timestamp] = price
		} else if msgType == 'Q' {
			var minTime, maxTime int32
			binary.Read(conn, binary.BigEndian, &minTime)
			binary.Read(conn, binary.BigEndian, &maxTime)
			fmt.Printf("Query: %d %d\n", minTime, maxTime)
			itemCount := 0
			totalCount := int64(0)
			for key, value := range data {
				if minTime <= key && key <= maxTime {
					itemCount += 1
					totalCount += int64(value)
				}
			}
			var result int32
			if itemCount > 0 {
				result = int32(totalCount / int64(itemCount))
			} else {
				result = 0
			}

			binary.Write(conn, binary.BigEndian, &result)
		}

	}
}
