package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	gouser "os/user"
	"path"
	"strings"

	"golang.org/x/crypto/ssh/terminal"

	"github.com/cernbox/go-cs3apis/cs3/auth/v0alpha"
	"github.com/cernbox/go-cs3apis/cs3/rpc"
	"github.com/cernbox/go-cs3apis/cs3/storageprovider/v0alpha"
	"google.golang.org/grpc"

	"github.com/pkg/errors"
)

var REVA_SERVER = os.Getenv("REVA_SERVER")

func main() {
	// Subcommands
	fsCommand := flag.NewFlagSet("peter", flag.ExitOnError)
	shareCommand := flag.NewFlagSet("share", flag.ExitOnError)
	linkCommand := flag.NewFlagSet("share", flag.ExitOnError)
	loginCommand := flag.NewFlagSet("login", flag.ExitOnError)
	whoamiCommand := flag.NewFlagSet("whoami", flag.ExitOnError)

	mkdirCommand := flag.NewFlagSet("mkdir", flag.ExitOnError)
	lsCommand := flag.NewFlagSet("ls", flag.ExitOnError)

	//whoamiCommand flags
	whoamiToken := whoamiCommand.String("token", "", "access token to use")

	//lsCommand flags
	lsCommandLongListing := lsCommand.Bool("l", false, "prints long listing with more info (size, mtime,...)")

	// Login subcommand flag pointers
	// netrcPtr := loginCommand.String("netrc", "", ".netrc file (Required)")

	// Verify that a subcommand has been provided
	// os.Arg[0] is the main command
	// os.Arg[1] will be the subcommand
	if len(os.Args) < 2 {
		fmt.Println(mainUsage)
		os.Exit(1)
	}

	if REVA_SERVER == "" {
		fmt.Println("REVA_SERVER is not set")
		os.Exit(1)
	}

	// Switch on the subcommand
	// Parse the flags for appropriate FlagSet
	// FlagSet.Parse() requires a set of arguments to parse as input
	// os.Args[2:] will be all arguments starting after the subcommand at os.Args[1]
	switch os.Args[1] {
	case "ls":
		lsCommand.Parse(os.Args[2:])
	case "mkdir":
		mkdirCommand.Parse(os.Args[2:])
	case "peter":
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

	if lsCommand.Parsed() {
		mds, err := list("/")
		if err != nil {
			log.Fatal(err)
			os.Exit(1)
		}
		for _, md := range mds {
			if *lsCommandLongListing {
				fmt.Printf("%+v %d %d %s\n", md.Permissions, md.Mtime, md.Size, md.Filename)
			} else {
				fmt.Println(md.Filename)
			}
		}
	}

	if mkdirCommand.Parsed() {
		fmt.Println("mkdir")
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
		var username, password string

		// check if username and password are set
		if loginCommand.NArg() >= 2 {
			username = loginCommand.Args()[0]
			password = loginCommand.Args()[1]
		} else {
			reader := bufio.NewReader(os.Stdin)
			fmt.Print("username: ")
			text, err := read(reader)
			if err != nil {
				log.Fatal("error reading input: ", err)
				os.Exit(1)
			}
			username = text

			fmt.Print("password: ")
			text, err = readPassword(0) // stdin
			if err != nil {
				log.Fatal("error reading input: ", err)
				os.Exit(1)
			}
			password = text
			fmt.Println("")
		}

		// authenticate to reva server
		token, err := authenticate(username, password)
		if err != nil {
			log.Fatal(err)
			os.Exit(1)
		}

		writeToken(token)
		fmt.Println("OK")
	}

	if whoamiCommand.Parsed() {
		var token string
		if whoamiCommand.NArg() != 0 {
			whoamiCommand.PrintDefaults()
			os.Exit(1)
		}

		if *whoamiToken != "" {
			token = *whoamiToken
		} else {
			// read token from file
			t, err := readToken()
			if err != nil {
				log.Fatal(err)
			}
			token = t
		}

		me, err := whoami(token)
		if err != nil {
			log.Fatal(err)
			os.Exit(1)
		}

		fmt.Printf("username: %s\ndisplay_name: %s\nmail: %s\ngroups: %v\n", me.Username, me.DisplayName, me.Mail, me.Groups)
	}
}

func getTokenFile() string {
	user, err := gouser.Current()
	if err != nil {
		panic(err)
	}

	return path.Join(user.HomeDir, ".reva")
}

func writeToken(token string) {
	ioutil.WriteFile(getTokenFile(), []byte(token), 0600)
}

func readToken() (string, error) {
	data, err := ioutil.ReadFile(getTokenFile())
	if err != nil {
		return "", err
	}
	return string(data), nil
}

var mainUsage = `Command line interface to REVA

Available commands:
  login     login to reva server
  whoami    tells who you are
  mkdir     create a directory
  move      moves a file/directory
  rm        removes a file/directory
  ls        list a directory
  stat      retrieves metadata for a file/directory


  

Authors: hugo.gonzalez.labrador@cern.ch
Copyright CERN-IT Storage Group 2018
`

func read(r *bufio.Reader) (string, error) {
	text, err := r.ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(text), nil
}
func readPassword(fd int) (string, error) {
	bytePassword, err := terminal.ReadPassword(fd)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(bytePassword)), nil
}

func authenticate(username, password string) (string, error) {
	client, err := getAuthClient()
	if err != nil {
		return "", err
	}

	req := &authv0alphapb.GenerateAccessTokenRequest{
		Username: username,
		Password: password,
	}

	ctx := context.Background()
	res, err := client.GenerateAccessToken(ctx, req)
	if err != nil {
		return "", err
	}

	if res.Status.Code != rpcpb.Code_CODE_OK {
		return "", apiError(res.Status)
	}
	return res.AccessToken, nil
}

func getStorageProviderClient() (storageproviderv0alphapb.StorageProviderServiceClient, error) {
	conn, err := getConn()
	if err != nil {
		return nil, err
	}
	return storageproviderv0alphapb.NewStorageProviderServiceClient(conn), nil
}

func getAuthClient() (authv0alphapb.AuthServiceClient, error) {
	conn, err := getConn()
	if err != nil {
		return nil, err
	}
	return authv0alphapb.NewAuthServiceClient(conn), nil
}

func getConn() (*grpc.ClientConn, error) {
	return grpc.Dial(REVA_SERVER, grpc.WithInsecure())
}

func list(fn string) ([]*storageproviderv0alphapb.Metadata, error) {
	client, err := getStorageProviderClient()
	if err != nil {
		return nil, err
	}

	req := &storageproviderv0alphapb.ListRequest{
		Filename: fn,
	}

	ctx := context.Background()
	stream, err := client.List(ctx, req)
	if err != nil {
		return nil, err
	}

	mds := []*storageproviderv0alphapb.Metadata{}
	for {
		res, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if res.Status.Code != rpcpb.Code_CODE_OK {
			return nil, apiError(res.Status)
		}
		mds = append(mds, res.Metadata)
	}
	return mds, nil
}

func whoami(token string) (*authv0alphapb.User, error) {
	client, err := getAuthClient()
	if err != nil {
		return nil, err
	}

	req := &authv0alphapb.WhoAmIRequest{AccessToken: token}

	ctx := context.Background()
	res, err := client.WhoAmI(ctx, req)
	if err != nil {
		return nil, err
	}

	if res.Status.Code != rpcpb.Code_CODE_OK {
		return nil, apiError(res.Status)
	}

	return res.User, nil
}

func apiError(status *rpcpb.Status) error {
	return errors.New(fmt.Sprintf("apierror: code=%v msg=%s", status.Code, status.Message))
}
