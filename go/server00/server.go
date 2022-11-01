package main

import (
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
	buf := make([]byte, 1)
	for {
		// read client request data

		_, err := conn.Read(buf)
		if err != nil {
			return
		}
		_, err = conn.Write(buf)
		if err != nil {
			return
		}
	}
}
