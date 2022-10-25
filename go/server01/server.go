// Copyright 2018 Venil Noronha. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/go-playground/validator/v10"
	"math/big"
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

type PrimeRequest struct {
	Method string   `json:"method"`
	Number *float64 `json:"number" validate:"required,numeric"`
}
type PrimeResponse struct {
	Method string `json:"method"`
	Prime  bool   `json:"prime"`
}

// handleConnection handles the lifetime of a connection
func handleConnection(conn net.Conn) {
	defer conn.Close()
	println("New connection")
	reader := bufio.NewReader(conn)

	for {
		bytes, err := reader.ReadBytes(byte('\n'))
		if err != nil {
			return
		}
		fmt.Printf("Bytes: %s", bytes)
		var parsedJson PrimeRequest
		err = json.Unmarshal(bytes, &parsedJson)
		if err != nil {
			fmt.Printf("Error: %s\n", err.Error())
			conn.Write([]byte("Error Parsing Json"))
			return
		}

		validate := validator.New()
		err = validate.Struct(parsedJson)
		if err != nil {
			fmt.Printf("Error: %s\n", err.Error())
			conn.Write([]byte("Error Validating Json"))
			return
		}
		if parsedJson.Method != "isPrime" {
			fmt.Printf("Error: Method set Incorrectly\n")
			conn.Write([]byte("Error: Method set Incorrectly"))
			return
		}
		prime := false
		if big.NewFloat(*parsedJson.Number).IsInt() {
			prime = big.NewInt(int64(*parsedJson.Number)).ProbablyPrime(0)
		} else {
			prime = false
		}

		response := &PrimeResponse{Method: "isPrime", Prime: prime}
		data, _ := json.Marshal(&response)
		fmt.Printf("Response: %s\n\n", data)
		conn.Write(data)
		conn.Write([]byte("\n"))

	}

}
