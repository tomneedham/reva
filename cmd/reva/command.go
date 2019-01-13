package main

import (
	"flag"
	"fmt"
)

// command is the representation to create commands
type command struct {
	*flag.FlagSet
	Name   string
	Action func() error
	Usage  func() string
}

// newCommand creates a new command
func newCommand(name string) *command {
	fs := flag.NewFlagSet(name, flag.ExitOnError)
	cmd := &command{
		Name: name,
		Usage: func() string {
			return "TODO"
		},
		Action: func() error {
			fmt.Println("hello")
			return nil
		},
		FlagSet: fs,
	}
	return cmd
}
