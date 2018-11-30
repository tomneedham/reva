package main

import (
	"flag"
	"fmt"
	"os"
)

func main() {

	// Subcommands
	fsCommand := flag.NewFlagSet("fs", flag.ExitOnError)
	shareCommand := flag.NewFlagSet("share", flag.ExitOnError)
	linkCommand := flag.NewFlagSet("share", flag.ExitOnError)
	loginCommand := flag.NewFlagSet("login", flag.ExitOnError)
	whoamiCommand := flag.NewFlagSet("whoami", flag.ExitOnError)

	// Login subcommand flag pointers
	netrcPtr := loginCommand.String("netrc", "", ".netrc file (Required)")

	// Verify that a subcommand has been provided
	// os.Arg[0] is the main command
	// os.Arg[1] will be the subcommand
	if len(os.Args) < 2 {
		fmt.Printf(mainUsage, os.Args[0])
		os.Exit(1)
	}

	// Switch on the subcommand
	// Parse the flags for appropriate FlagSet
	// FlagSet.Parse() requires a set of arguments to parse as input
	// os.Args[2:] will be all arguments starting after the subcommand at os.Args[1]
	switch os.Args[1] {
	case "fs":
		fsCommand.Parse(os.Args[2:])
	case "share":
		shareCommand.Parse(os.Args[2:])
	case "link":
		linkCommand.Parse(os.Args[2:])
	case "login":
		loginCommand.Parse(os.Args[2:])
	case "whoami":
		whoamiCommand.Parse(os.Args[2:])
	default:
		flag.PrintDefaults()
		os.Exit(1)
	}

	if fsCommand.Parsed() {
		fmt.Println("fs magic")
		os.Exit(1)
	}

	if shareCommand.Parsed() {
		fmt.Println("share unicorns")
		os.Exit(1)
	}

	if linkCommand.Parsed() {
		fmt.Println("link sorcery")
		os.Exit(1)
	}

	if loginCommand.Parsed() {
		if *netrcPtr == "" {
			loginCommand.PrintDefaults()
			os.Exit(1)
		}

		// check if username and password are set
		if loginCommand.NArg() >= 2 {
			username := loginCommand.Args()[0]
			password := loginCommand.Args()[1]

			fmt.Println("Welcome to REVA: " + username)
			fmt.Println("Using password: " + password)
			os.Exit(0)
		}
	}

	if whoamiCommand.Parsed() {
		fmt.Println("whoami maybe")
		os.Exit(1)
	}
}

var mainUsage = `REVA CLI
CERN-IT Storage Group 2018

Usage: %s

  login     login to reva server
  whoami    tells who you are     
`
