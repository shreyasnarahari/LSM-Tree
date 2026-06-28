package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/shreyas/lsmtree/db"
)

func main() {
	fmt.Println("LSM-Tree REPL (Type 'help' for commands, 'exit' to quit)")

	dir := "./data"
	fmt.Printf("Opening database at %s...\n", dir)

	database, err := db.Open(db.DBOptions{
		Dir:          dir,
		MemTableSize: 4 * 1024 * 1024,
		SyncOnWrite:  false,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening db: %v\n", err)
		os.Exit(1)
	}
	defer database.Close()

	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("lsm> ")
		if !scanner.Scan() {
			break
		}

		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		parts := strings.SplitN(line, " ", 3)
		cmd := strings.ToLower(parts[0])

		switch cmd {
		case "put":
			if len(parts) < 3 {
				fmt.Println("Usage: put <key> <value>")
				continue
			}
			err := database.Put([]byte(parts[1]), []byte(parts[2]))
			if err != nil {
				fmt.Printf("Error: %v\n", err)
			} else {
				fmt.Println("OK")
			}

		case "get":
			if len(parts) < 2 {
				fmt.Println("Usage: get <key>")
				continue
			}
			val, err := database.Get([]byte(parts[1]))
			if err == db.ErrKeyNotFound {
				fmt.Println("(nil)")
			} else if err != nil {
				fmt.Printf("Error: %v\n", err)
			} else {
				fmt.Printf("%s\n", string(val))
			}

		case "delete":
			if len(parts) < 2 {
				fmt.Println("Usage: delete <key>")
				continue
			}
			err := database.Delete([]byte(parts[1]))
			if err != nil {
				fmt.Printf("Error: %v\n", err)
			} else {
				fmt.Println("OK")
			}

		case "stats":
			fmt.Printf("SSTables (L0): %d\n", database.SSTCount())
			fmt.Printf("Immutable MemTables: %d\n", database.ImmutableCount())

		case "help":
			fmt.Println("Commands:")
			fmt.Println("  put <key> <value>  - Insert or update a key")
			fmt.Println("  get <key>          - Retrieve a value by key")
			fmt.Println("  delete <key>       - Delete a key")
			fmt.Println("  stats              - Show database statistics")
			fmt.Println("  exit, quit         - Exit the REPL")

		case "exit", "quit":
			fmt.Println("Closing database...")
			return

		default:
			fmt.Printf("Unknown command: %s\n", cmd)
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "Error reading standard input: %v\n", err)
	}
}
