package eosclient

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"
)

var opt = &Options{
	URL:       "root://eoshome-g.cern.ch",
	LogOutput: os.Stdout,
}

var client, _ = New(opt)

var username = "gonzalhu"
var home = "/eos/user/g/gonzalhu"

func TestList(t *testing.T) {
	var ctx, cancel = context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	entries, err := client.List(ctx, username, home)
	if err != nil {
		t.Fatal()
		return
	}
	for _, e := range entries {
		fmt.Println(e)
	}
}

func TestCreateDir(t *testing.T) {
	var ctx, cancel = context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	for i := 0; i < 10; i++ {
		err := client.CreateDir(ctx, username, fmt.Sprintf("%s/test-create-dir/test-%d", home, i))
		if err != nil {
			t.Fatal(err)
			return
		}
	}
}

func TestListRecycle(t *testing.T) {
	var ctx, cancel = context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	_, err := client.ListDeletedEntries(ctx, username)
	if err != nil {
		t.Fatal(err)
		return
	}
}
func TestQuota(t *testing.T) {
	var ctx, cancel = context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	max, used, err := client.GetQuota(ctx, username, "/eos/user/l/labradorsvc")
	if err != nil {
		t.Fatal(err)
		return
	}
	fmt.Println(max, used)

}
