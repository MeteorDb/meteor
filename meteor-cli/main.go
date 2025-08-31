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
	cli.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	n, err := cli.conn.Read(buffer)
	if err != nil {
		return "", fmt.Errorf("failed to read response: %v", err)
	}

	response := strings.TrimSpace(string(buffer[:n]))
	return response, nil
}

// ParseCommand parses user input and handles transaction state
func (cli *MeteorCLI) ParseCommand(input string) (string, error) {
	input = strings.TrimSpace(input)
	if input == "" {
		return "", nil
	}

	parts := strings.Fields(input)
	operation := strings.ToUpper(parts[0])

	switch operation {
	case "BEGIN":
		return cli.handleBegin(parts)
	case "COMMIT":
		return cli.handleCommit(parts)
	case "ROLLBACK":
		return cli.handleRollback(parts)
	case "PUT":
		return cli.handlePut(parts)
	case "GET":
		return cli.handleGet(parts)
	case "DELETE":
		return cli.handleDelete(parts)
	case "HELP", "\\H":
		cli.showHelp()
		return "", nil
	case "QUIT", "\\Q", "EXIT":
		return "QUIT", nil
	case "STATUS":
		cli.showStatus()
		return "", nil
	default:
		return "", fmt.Errorf("unknown command: %s. Type 'help' for available commands", operation)
	}
}

// handleBegin processes BEGIN command
func (cli *MeteorCLI) handleBegin(parts []string) (string, error) {
	if cli.txnState.InTransaction {
		return "", fmt.Errorf("already in transaction %s. COMMIT or ROLLBACK first", cli.txnState.TransactionID)
	}

	// Forward the command as-is to the server
	command := strings.Join(parts, " ")
	response, err := cli.SendCommand(command)
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
func (cli *MeteorCLI) handleCommit(parts []string) (string, error) {
	if err := cli.validateTransactionState(true, "commit"); err != nil {
		return "", err
	}

	command := fmt.Sprintf("COMMIT %s", cli.txnState.TransactionID)
	return cli.executeCommandWithErrorHandling(command, CommandTypeTransactionEnd)
}

// handleRollback processes ROLLBACK command
func (cli *MeteorCLI) handleRollback(parts []string) (string, error) {
	if err := cli.validateTransactionState(true, "rollback"); err != nil {
		return "", err
	}

	command := fmt.Sprintf("ROLLBACK %s", cli.txnState.TransactionID)
	return cli.executeCommandWithErrorHandling(command, CommandTypeTransactionEnd)
}

// handlePut processes PUT command
func (cli *MeteorCLI) handlePut(parts []string) (string, error) {
	if err := cli.validateArgumentCount(parts, 3, "PUT command requires key and value: PUT <key> <value>"); err != nil {
		return "", err
	}

	command := cli.buildCommandWithTransactionID(parts)
	return cli.executeCommandWithErrorHandling(command, CommandTypeData)
}

// handleGet processes GET command
func (cli *MeteorCLI) handleGet(parts []string) (string, error) {
	if err := cli.validateArgumentCount(parts, 2, "GET command requires key: GET <key>"); err != nil {
		return "", err
	}

	command := cli.buildCommandWithTransactionID(parts)
	return cli.executeCommandWithErrorHandling(command, CommandTypeData)
}

// handleDelete processes DELETE command
func (cli *MeteorCLI) handleDelete(parts []string) (string, error) {
	if err := cli.validateArgumentCount(parts, 2, "DELETE command requires key: DELETE <key>"); err != nil {
		return "", err
	}

	command := cli.buildCommandWithTransactionID(parts)
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

// buildCommandWithTransactionID builds a command string, adding transaction ID if in transaction
func (cli *MeteorCLI) buildCommandWithTransactionID(parts []string) string {
	if !cli.txnState.InTransaction || len(parts) == 0 {
		// Not in transaction or invalid parts - use command as-is
		return strings.Join(parts, " ")
	}

	// Add transaction ID to the command for transactional operations
	return fmt.Sprintf("%s %s", strings.Join(parts, " "), cli.txnState.TransactionID)
}

// validateTransactionState validates if we're in the expected transaction state
func (cli *MeteorCLI) validateTransactionState(requiredState bool, action string) error {
	if requiredState && !cli.txnState.InTransaction {
		return fmt.Errorf("no active transaction to %s", action)
	}
	return nil
}

// validateArgumentCount validates the number of command arguments
func (cli *MeteorCLI) validateArgumentCount(parts []string, minArgs int, usage string) error {
	if len(parts) < minArgs {
		return fmt.Errorf("%s", usage)
	}
	return nil
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
