package main

import (
	"fmt"
	"log"
	"os"
)

var (
	conf *config
)

func main() {

	cmds := []*command{
		configureCommand(),
		loginCommand(),
		whoamiCommand(),
		lsCommand(),
		statCommand(),
		uploadCommand(),
		rmCommand(),
		mkdirCommand(),
	}

	// Verify that a subcommand has been provided
	// os.Arg[0] is the main command
	// os.Arg[1] will be the subcommand
	if len(os.Args) < 2 {
		fmt.Println(mainUsage)
		os.Exit(1)
	}

	// Verify a configuration file exists.
	// If if does not, create one
	c, err := readConfig()
	if err != nil && os.Args[1] != "configure" {
		fmt.Println("reva is not initialized, run \"reva configure\"")
		os.Exit(1)
	} else {
		if os.Args[1] != "configure" {
			conf = c
		}
	}

	// Run command
	action := os.Args[1]
	for _, v := range cmds {
		if v.Name == action {
			v.Parse(os.Args[2:])
			err := v.Action()
			if err != nil {
				log.Fatal(err)
				os.Exit(1)
			}
			os.Exit(0)
		}
	}

	// command not found
	fmt.Println(mainUsage)
	os.Exit(1)
}

var mainUsage = `Command line interface to REVA

Available commands:
  configure configure the reva client
  login     login to reva server
  whoami    tells who you are
  mkdir     create a directory
  move      moves a file/directory
  rm        removes a file/directory
  ls        list a directory
  stat      retrieves metadata for a file/directory
  upload    upload a local file to the remote server
  download  download a remote file to the local filesystem
  

Authors: hugo.gonzalez.labrador@cern.ch
Copyright CERN-IT Storage Group 2018
`
