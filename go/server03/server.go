package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"unicode"
)

type UserList struct {
	sync.RWMutex
	m map[string]bufio.Writer
}

// main serves as the program entry point
func main() {

	listener, err := net.Listen("tcp", "0.0.0.0:40000")
	if err != nil {
		fmt.Println("failed to create listener, err:", err)
		os.Exit(1)
	}
	fmt.Printf("listening on %s\n", listener.Addr())
	users := UserList{m: make(map[string]bufio.Writer)}

	// listen for new connections
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("failed to accept connection, err:", err)
			continue
		}
		// pass an accepted connection to a handler goroutine
		go handleConnection(conn, &users)
	}
}

func ValidUsername(username string) bool {
	nonLetter := func(c rune) bool { return !unicode.IsLetter(c) && !unicode.IsNumber(c) }
	words := strings.FieldsFunc(username, nonLetter)
	return username == strings.Join(words, "")
}

// handleConnection handles the lifetime of a connection
func handleConnection(conn net.Conn, users *UserList) {
	defer conn.Close()
	println("New connection")

	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	writer.WriteString("Welcome to budgetchat! What shall I call you?\n")
	writer.Flush()

	username, _ := reader.ReadString(byte('\n'))
	username = strings.TrimSpace(username)

	if !ValidUsername(username) || len(username) == 0 {
		println("Kicked invalid user")
		return
	}

	fmt.Printf("user: %s\n", username)
	users.Lock()
	var userArray []string
	for k := range users.m {
		userArray = append(userArray, k)
	}
	users.m[username] = *writer
	users.Unlock()
	writer.WriteString(fmt.Sprintf("* The room contains: %s\n", strings.Join(userArray, ", ")))
	writer.Flush()

	broadcast(username, users, fmt.Sprintf("* %s has entered the room\n", username))

	for {
		chatMessage, err := reader.ReadString(byte('\n'))
		if err != nil {
			broadcast(username, users, fmt.Sprintf("* %s has left the room\n", username))
			users.Lock()
			delete(users.m, username)
			users.Unlock()
			return
		}
		broadcast(username, users, fmt.Sprintf("[%s] %s", username, chatMessage))
	}

}

func broadcast(sourceUser string, users *UserList, message string) {
	fmt.Print(message)
	users.RLock()
	for user, writer := range users.m {
		if user != sourceUser {
			_, err := writer.WriteString(message)
			if err != nil {
				continue
			}
			writer.Flush()
		}
	}
	users.RUnlock()
}
