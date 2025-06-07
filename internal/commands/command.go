package commands

import (
	"log"
	"net"
	"strings"
	"time"

	"meteor/internal/common"
	"meteor/internal/dbmanager"
)

// ArgSpec describes exactly one positional argument
type ArgSpec struct {
    Name        string
    Type        string
    Required    bool
    Description string
}

// CommandSpec is what each file builds and calls Register on
type CommandSpec struct {
    Name    string
    Args    []ArgSpec
    Handler func(dm *dbmanager.DBManager, cmd *common.Command) ([]byte, error)
}

type CommandContext struct {
	clientConnection *net.Conn
}


var registry = make(map[string]*CommandSpec)

// Register wires up your CommandSpec into the global registry
func Register[I any](
    name    string,
    args    []ArgSpec,
    ensureInputs func(*dbmanager.DBManager, *common.Command) (I, error),
    execute  func(*dbmanager.DBManager, I, *CommandContext) ([]byte, error),
) {
	if name == "" {
		log.Fatalf("command name cannot be empty")
	}

	if _, ok := registry[name]; ok {
		log.Fatalf("command with name %q already registered", name)
	}

    if ensureInputs == nil {
        log.Fatalf("command %q must supply ensureInput function", name)
    }
    if execute == nil {
        log.Fatalf("command %q must supply execute function", name)
    }

    handler := func(dm *dbmanager.DBManager, cmd *common.Command) ([]byte, error) {
        log.Printf("[CMD][%s] validating %v", name, cmd.Args)
        in, err := ensureInputs(dm, cmd)
        if err != nil {
            log.Printf("[CMD][%s] validation failed: %v", name, err)
            return nil, err
        }

        log.Printf("[CMD][%s] START executing %v", name, cmd.Args)
        t0 := time.Now()
        res, err := execute(dm, in, &CommandContext{clientConnection: cmd.Connection})
        dt := time.Since(t0)

        if err != nil {
            log.Printf("[CMD][%s] ERROR after %v: %v", name, dt, err)
        } else {
            log.Printf("[CMD][%s] DONE in %v", name, dt)
        }
        return res, err
    }

	uppercasedName := strings.ToUpper(name)

    registry[uppercasedName] = &CommandSpec{
        Name:    name,
        Args:    args,
        Handler: handler,
    }
}

// Get returns the CommandSpec registered under name, if any
func Get(name string) (*CommandSpec, bool) {
	uppercasedName := strings.ToUpper(name)
    c, ok := registry[uppercasedName]
    return c, ok
}
