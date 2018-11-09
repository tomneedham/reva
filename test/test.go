package main

import (
	"context"
	"fmt"
	"os"

	"github.com/cernbox/reva/pkg/storage"
	"github.com/cernbox/reva/pkg/storage/eos"
	//"github.com/cernbox/reva/pkg/storage/local"
	"github.com/cernbox/reva/pkg/logger"
	"github.com/cernbox/reva/pkg/user"

	"github.com/pkg/errors"
)

func main() {

	u := &user.User{
		Account: "gonzalhu",
		Groups:  []string{"cernbox-admins"},
	}

	logger := logger.New(os.Stdout, "main", "tracekey")

	driver := getStorage()
	ctx := context.Background()
	ctx = user.ContextSetUser(ctx, u)
	ctx = context.WithValue(ctx, "tracekey", "7fa22e71-964b-4f6f-8042-6083460e5555")

	mds, err := driver.ListFolder(ctx, "/")
	if err != nil {
		logger.Logf(ctx, "error: %s", err.Error())
		if err, ok := err.(stackTracer); ok {
			for _, f := range err.StackTrace() {
				logger.Logf(ctx, "stack frame: %s:%d %n", f, f, f)
			}
		}
		os.Exit(1)
	}

	for _, md := range mds {
		fmt.Println(md.Path)
	}
}

func getStorage() storage.Storage {
	/*
		localDriver := local.New(&local.Options{
			Namespace: "/tmp",
		})
		return localDriver
	*/
	eosDriver, _ := eos.New(&eos.Options{
		EnableLogging: true,
		LogOut:        os.Stdout,
		MasterURL:     "root://eosuser-internal.cern.ch",
		Namespace:     "/eos/project/cerenbox/",
		EosBinary:     "/usr/bin/eos",
		LogKey:        "tracekey",
	})
	return eosDriver
}

type stackTracer interface {
	StackTrace() errors.StackTrace
}
