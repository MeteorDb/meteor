package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

// TransactionState tracks the current transaction state
type TransactionState struct {
	InTransaction bool
	TransactionID string
}

// MeteorCLI represents the CLI client
type MeteorCLI struct {
	conn      net.Conn
	txnState  TransactionState
	host      string
	port      string
	connected bool
	prompt    string
	reader    *bufio.Reader
}

// NewMeteorCLI creates a new CLI instance
func NewMeteorCLI(host, port string) *MeteorCLI {
	return &MeteorCLI{
		host:   host,
		port:   port,
		prompt: "meteor> ",
		reader: bufio.NewReader(os.Stdin),
	}
}

// Connect establishes connection to the meteor database
func (cli *MeteorCLI) Connect() error {
	conn, err := net.Dial("tcp", cli.host+":"+cli.port)
	if err != nil {
		return fmt.Errorf("failed to connect to meteor database: %v", err)
	}
	cli.conn = conn
	cli.connected = true
	fmt.Printf("Connected to meteor database at %s:%s\n", cli.host, cli.port)
	return nil
}

// Disconnect closes the connection
func (cli *MeteorCLI) Disconnect() {
	if cli.connected && cli.conn != nil {
		cli.conn.Close()
		cli.connected = false
		fmt.Println("Disconnected from meteor database")
	}
}

// SendCommand sends a command to the meteor database and returns the response
func (cli *MeteorCLI) SendCommand(command string) (string, error) {
	if !cli.connected {
		return "", fmt.Errorf("not connected to database")
	}

	// Send command
	_, err := cli.conn.Write([]byte(command))
	if err != nil {
		return "", fmt.Errorf("failed to send command: %v", err)
	}

	// Read response
	buffer := make([]byte, 4096)
	cli.conn.SetReadDeadline(time.Now().Add(60 * time.Second)) // 1 minute timeout
	n, err := cli.conn.Read(buffer)
	if err != nil {
		return "", fmt.Errorf("failed to read response: %v", err)
	}

	response := strings.TrimSpace(string(buffer[:n]))
	return response, nil
}

// ParseCommand handles transaction state and forwards commands to server
func (cli *MeteorCLI) ParseCommand(input string) (string, error) {
	input = strings.TrimSpace(input)
	if input == "" {
		return "", nil
	}

	// Extract just the operation name for client-side handling
	firstSpace := strings.Index(input, " ")
	var operation string
	if firstSpace == -1 {
		operation = strings.ToUpper(input)
	} else {
		operation = strings.ToUpper(input[:firstSpace])
	}

	// Handle client-only commands
	switch operation {
	case "HELP", "\\H":
		cli.showHelp()
		return "", nil
	case "QUIT", "\\Q", "EXIT":
		return "QUIT", nil
	case "STATUS":
		cli.showStatus()
		return "", nil
	}

	// Handle transaction commands with state tracking
	switch operation {
	case "BEGIN":
		return cli.handleBegin(input)
	case "COMMIT":
		return cli.handleCommit(input)
	case "ROLLBACK":
		return cli.handleRollback(input)
	default:
		// All other commands are forwarded with transaction ID if needed
		return cli.handleDataCommand(input)
	}
}

// handleBegin processes BEGIN command
func (cli *MeteorCLI) handleBegin(input string) (string, error) {
	if cli.txnState.InTransaction {
		return "", fmt.Errorf("already in transaction %s. COMMIT or ROLLBACK first", cli.txnState.TransactionID)
	}

	// Forward the raw command to the server
	response, err := cli.SendCommand(input)
	if err != nil {
		return "", err
	}

	// Check if response is an error
	if strings.HasPrefix(response, "error:") {
		return response, nil
	}

	// Extract transaction ID from response
	txnID := strings.TrimSpace(response)
	if _, err := strconv.Atoi(txnID); err != nil {
		return "", fmt.Errorf("invalid transaction ID received: %s", txnID)
	}

	cli.txnState.InTransaction = true
	cli.txnState.TransactionID = txnID
	cli.updatePrompt()

	return fmt.Sprintf("Transaction %s started", txnID), nil
}

// handleCommit processes COMMIT command
func (cli *MeteorCLI) handleCommit(input string) (string, error) {
	if !cli.txnState.InTransaction {
		return "", fmt.Errorf("no active transaction to commit")
	}

	// Send COMMIT with transaction ID
	command := fmt.Sprintf("COMMIT %s", cli.txnState.TransactionID)
	return cli.executeCommandWithErrorHandling(command, CommandTypeTransactionEnd)
}

// handleRollback processes ROLLBACK command
func (cli *MeteorCLI) handleRollback(input string) (string, error) {
	if !cli.txnState.InTransaction {
		return "", fmt.Errorf("no active transaction to rollback")
	}

	// Send ROLLBACK with transaction ID
	command := fmt.Sprintf("ROLLBACK %s", cli.txnState.TransactionID)
	return cli.executeCommandWithErrorHandling(command, CommandTypeTransactionEnd)
}

// handleDataCommand forwards data commands to server with transaction ID if needed
func (cli *MeteorCLI) handleDataCommand(input string) (string, error) {
	var command string
	if cli.txnState.InTransaction {
		// Add transaction ID to the command for transactional operations
		command = fmt.Sprintf("%s %s", input, cli.txnState.TransactionID)
	} else {
		// Send command as-is for non-transactional operations
		command = input
	}
	
	return cli.executeCommandWithErrorHandling(command, CommandTypeData)
}

// CommandType represents the type of database command
type CommandType int

const (
	CommandTypeTransactionEnd CommandType = iota // COMMIT, ROLLBACK
	CommandTypeData                              // PUT, GET, DELETE
)

// clearTransactionState clears the CLI transaction state and updates prompt
func (cli *MeteorCLI) clearTransactionState() {
	cli.txnState.InTransaction = false
	cli.txnState.TransactionID = ""
	cli.updatePrompt()
}

// executeCommandWithErrorHandling executes a command with consistent error handling
func (cli *MeteorCLI) executeCommandWithErrorHandling(command string, cmdType CommandType) (string, error) {
	response, err := cli.SendCommand(command)
	if err != nil {
		// Clear transaction state on network/send error
		if cmdType == CommandTypeTransactionEnd || cli.txnState.InTransaction {
			cli.clearTransactionState()
		}
		return "", err
	}

	// Check if response is an error
	if strings.HasPrefix(response, "error:") {
		// Clear transaction state on server error
		if cmdType == CommandTypeTransactionEnd || cli.txnState.InTransaction {
			cli.clearTransactionState()
		}
		return response, nil
	}

	// Clear transaction state for successful transaction-ending commands
	if cmdType == CommandTypeTransactionEnd {
		cli.clearTransactionState()
	}

	return response, nil
}


// updatePrompt updates the CLI prompt based on transaction state
func (cli *MeteorCLI) updatePrompt() {
	if cli.txnState.InTransaction {
		cli.prompt = fmt.Sprintf("meteor[%s]> ", cli.txnState.TransactionID)
	} else {
		cli.prompt = "meteor> "
	}
}

// showHelp displays available commands
func (cli *MeteorCLI) showHelp() {
	fmt.Println("Meteor Database CLI Commands:")
	fmt.Println("  BEGIN [isolation_level]  - Start a new transaction")
	fmt.Println("                             Valid isolation levels: READ_COMMITTED (default),")
	fmt.Println("                             REPEATABLE_READ, SNAPSHOT_ISOLATION, SERIALIZABLE")
	fmt.Println("  PUT <key> <value>        - Insert or update a key-value pair")
	fmt.Println("  GET <key>                - Retrieve value for a key")
	fmt.Println("  DELETE <key>             - Delete a key")
	fmt.Println("  CGET \"<WHERE condition>\" - Conditional get with WHERE clause")
	fmt.Println("  RGET <startKey> <endKey> - Range get between start and end keys")
	fmt.Println("  COUNT [\"<WHERE condition>\"] - Count records, optionally with WHERE clause")
	fmt.Println("  SCAN <pattern> [\"<WHERE condition>\"] - Scan with pattern and optional filter")
	fmt.Println("  COMMIT                   - Commit current transaction")
	fmt.Println("  ROLLBACK                 - Rollback current transaction")
	fmt.Println("  STATUS                   - Show current transaction status")
	fmt.Println("  HELP                     - Show this help message")
	fmt.Println("  QUIT                     - Exit the CLI")
	fmt.Println("")
	fmt.Println("Notes:")
	fmt.Println("  - Commands outside transactions auto-commit")
	fmt.Println("  - Commands inside transactions are queued until COMMIT")
	fmt.Println("  - Transaction IDs are managed automatically")
	fmt.Println("  - Use quotes around conditions with spaces: CGET \"WHERE status = 'active user'\"")
	fmt.Println("  - Supports both single and double quotes in arguments")
}

// showStatus displays current transaction status
func (cli *MeteorCLI) showStatus() {
	if cli.txnState.InTransaction {
		fmt.Printf("In transaction: %s\n", cli.txnState.TransactionID)
	} else {
		fmt.Println("No active transaction")
	}
	fmt.Printf("Connected: %t\n", cli.connected)
	if cli.connected {
		fmt.Printf("Server: %s:%s\n", cli.host, cli.port)
	}
}

// Run starts the REPL
func (cli *MeteorCLI) Run() {
	fmt.Println("Meteor Database CLI")
	fmt.Println("Type 'help' for available commands or 'quit' to exit")
	fmt.Println()

	for {
		fmt.Print(cli.prompt)

		input, err := cli.reader.ReadString('\n')
		if err != nil {
			fmt.Printf("Error reading input: %v\n", err)
			continue
		}

		input = strings.TrimSpace(input)
		if input == "" {
			continue
		}

		response, err := cli.ParseCommand(input)
		if err != nil {
			fmt.Printf("Error: %v\n", err)
			continue
		}

		if response == "QUIT" {
			break
		}

		if response != "" {
			fmt.Println(response)
		}
	}
}


func main() {
	// Default connection parameters
	host := "localhost"
	port := "5050"

	// Check for command line arguments
	if len(os.Args) > 1 {
		if os.Args[1] == "--help" || os.Args[1] == "-h" {
			fmt.Println("Usage: meteor-cli [host] [port]")
			fmt.Println("Default: meteor-cli localhost 5050")
			return
		}
		host = os.Args[1]
	}
	if len(os.Args) > 2 {
		port = os.Args[2]
	}

	cli := NewMeteorCLI(host, port)

	// Connect to database
	if err := cli.Connect(); err != nil {
		fmt.Printf("Failed to connect: %v\n", err)
		fmt.Println("Make sure the meteor database server is running")
		os.Exit(1)
	}
	defer cli.Disconnect()

	// Start REPL
	cli.Run()
}
