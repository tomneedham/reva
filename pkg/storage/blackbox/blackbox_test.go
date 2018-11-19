package blackbox

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"testing"

	"github.com/cernbox/reva/pkg/storage"
	"github.com/cernbox/reva/pkg/storage/eos"
	"github.com/cernbox/reva/pkg/storage/local"
	"github.com/cernbox/reva/pkg/user"
	"github.com/gofrs/uuid"
)

var ctx = user.ContextSetUser(context.Background(), &user.User{Username: "gonzalhu"})
var testFolder = "/test-folder"
var contents = []byte("1234")
var targetEOSACLUser = "labradorsvc"

func newFile() io.ReadCloser {
	return ioutil.NopCloser(bytes.NewBuffer(contents))
}

func newFileWithSize(size int) io.ReadCloser {
	data := []byte{}
	for i := 0; i < size; i++ {
		data = append(data, 1)
	}
	return ioutil.NopCloser(bytes.NewBuffer(data))
}

func newFn() string {
	return path.Join(testFolder, uuid.Must(uuid.NewV4()).String())
}

var storages = map[string]storage.Storage{
	"local": local.New(&local.Options{Namespace: "/home/labkode/go/src/github.com/cernbox/reva/pkg/storage/blackbox"}),
	//"eos":   eos.New(&eos.Options{MasterURL: "root://eoshome-g.cern.ch", Namespace: "/eos/user/g/gonzalhu", LogOut: os.Stdout}),
	"eos": eos.New(&eos.Options{MasterURL: "root://eoshome-g.cern.ch", Namespace: "/eos/user/g/gonzalhu"}),
}

func TestMain(m *testing.M) {
	for _, s := range storages {
		s.Delete(ctx, testFolder)
		s.CreateDir(ctx, testFolder)
	}
	os.Exit(m.Run())
}

func TestStorageUpdateACL(t *testing.T) {
	fn := newFn()
	acl := &storage.ACL{Mode: storage.ACLModeReadOnly, Target: targetEOSACLUser, Type: storage.ACLTypeUser}
	for id, s := range storages {
		t.Run(id, func(t *testing.T) {
			s.CreateDir(ctx, fn)
			s.SetACL(ctx, fn, acl)

			updateACL := &storage.ACL{Mode: storage.ACLModeReadWrite, Target: targetEOSACLUser, Type: storage.ACLTypeUser}
			if err := s.UpdateACL(ctx, fn, updateACL); err != nil {
				t.Fatal(err)
			}

			acl, err := s.GetACL(ctx, fn, storage.ACLTypeUser, targetEOSACLUser)
			if err != nil {
				t.Fatal(err)
			}
			if acl.Target != targetEOSACLUser {
				t.Fatal(acl.Target)
			}
			if acl.Type != storage.ACLTypeUser {
				t.Fatal(acl.Type)
			}
			if acl.Mode != storage.ACLModeReadWrite {
				t.Fatal(acl.Mode)
			}

		})
	}
}

func TestStorageSetACL(t *testing.T) {
	fn := newFn()
	acl := &storage.ACL{Mode: storage.ACLModeReadOnly, Target: targetEOSACLUser, Type: storage.ACLTypeUser}
	for id, s := range storages {
		t.Run(id, func(t *testing.T) {
			s.CreateDir(ctx, fn)
			if err := s.SetACL(ctx, fn, acl); err != nil {
				t.Fatal(err)
			}
			acl, err := s.GetACL(ctx, fn, storage.ACLTypeUser, targetEOSACLUser)
			if err != nil {
				t.Fatal(err)
			}
			if acl.Target != targetEOSACLUser {
				t.Fatal(acl.Target)
			}
			if acl.Type != storage.ACLTypeUser {
				t.Fatal(acl.Type)
			}

		})
	}
}

func TestStorageRestoreRevision(t *testing.T) {
	fn := newFn()
	for id, s := range storages {
		t.Run(id, func(t *testing.T) {
			s.Upload(ctx, fn, newFileWithSize(10))
			s.Upload(ctx, fn, newFile()) // version created
			revs, err := s.ListRevisions(ctx, fn)
			if err != nil {
				t.Fatal(err)
			}
			rev := revs[0]
			err = s.RestoreRevision(ctx, fn, rev.RevKey)
			if err != nil {
				t.Fatal(err)
			}
			md, _ := s.GetMD(ctx, fn)
			if md.Size != uint64(10) {
				t.Fatal(md)
			}
		})
	}
}

func TestStorageDownloadRevision(t *testing.T) {
	fn := newFn()
	for id, s := range storages {
		t.Run(id, func(t *testing.T) {
			s.Upload(ctx, fn, newFile())
			s.Upload(ctx, fn, newFile()) // version created
			revs, _ := s.ListRevisions(ctx, fn)
			rev := revs[0]
			fd, err := s.DownloadRevision(ctx, fn, rev.RevKey)
			if err != nil {
				t.Fatal(err)
			}
			data, err := ioutil.ReadAll(fd)
			if err != nil {
				t.Fatal(err)
			}
			if len(contents) != len(data) {
				t.Fatal(len(data))
			}
		})
	}
}

func TestStorageListRevisions(t *testing.T) {
	fn := newFn()
	for id, s := range storages {
		t.Run(id, func(t *testing.T) {
			s.Upload(ctx, fn, newFile())
			s.Upload(ctx, fn, newFile()) // version created
			revs, err := s.ListRevisions(ctx, fn)
			if err != nil {
				t.Fatal(err)
			}
			if len(revs) != 1 {
				t.Fatal(len(revs))
			}
			rev := revs[0]
			if rev.Size != uint64(len(contents)) {
				t.Fatal(rev.Size)
			}
		})
	}
}

func TestStorageGetMD(t *testing.T) {
	fn := newFn()
	for id, s := range storages {
		t.Run(id, func(t *testing.T) {
			s.CreateDir(ctx, fn)
			md, err := s.GetMD(ctx, fn)
			if err != nil {
				t.Fatal(err)
			}
			if md.Path != fn {
				t.Fatal()
			}
		})
	}
}

func TestStorageGetPathByID(t *testing.T) {
	fn := newFn()
	for id, s := range storages {
		t.Run(id, func(t *testing.T) {
			s.CreateDir(ctx, fn)
			md, _ := s.GetMD(ctx, fn)
			p, err := s.GetPathByID(ctx, md.ID)
			if err != nil {
				t.Fatal(err)
			}
			if p != md.Path {
				t.Fatal(p, md)
			}
		})
	}

}

func TestStorageList(t *testing.T) {
	fn := newFn()
	for id, s := range storages {
		t.Run(id, func(t *testing.T) {
			if err := s.CreateDir(ctx, fn); err != nil {
				t.Fatal(err)
			}

			for i := range []int{1, 2, 3, 4} {
				fn := path.Join(fn, fmt.Sprintf("test-file-%d", i))
				s.CreateDir(ctx, fn)
			}
			mds, err := s.ListFolder(ctx, fn)
			if err != nil {
				t.Fatal(err)
			}
			if len(mds) != 4 {
				t.Fatal()
			}
		})
	}
}

func TestStorageUpload(t *testing.T) {
	fn := newFn()
	for id, s := range storages {
		t.Run(id, func(t *testing.T) {
			if err := s.Upload(ctx, fn, newFile()); err != nil {
				t.Fatal(err)
			}

			md, err := s.GetMD(ctx, fn)
			if err != nil {
				t.Fatal(err)
			}
			if uint64(len(contents)) != md.Size {
				t.Fatal(md.Size)
			}
		})
	}
}

func TestStorageMove(t *testing.T) {
	fn := newFn()
	targetFolder := newFn()
	for id, s := range storages {
		s.Delete(ctx, targetFolder)
		t.Run(id, func(t *testing.T) {
			s.CreateDir(ctx, fn)
			if err := s.Move(ctx, fn, targetFolder); err != nil {
				t.Fatal(err)
			}
			md, _ := s.GetMD(ctx, targetFolder)
			if md.Path != targetFolder {
				t.Fatal()
			}
			s.Delete(ctx, targetFolder)
		})
	}
}

func TestStorageDownload(t *testing.T) {
	fn := newFn()
	for id, s := range storages {
		t.Run(id, func(t *testing.T) {
			err := s.Upload(ctx, fn, newFile())
			fd, err := s.Download(ctx, fn)
			if err != nil {
				t.Fatal(err)
			}
			data, err := ioutil.ReadAll(fd)
			if err != nil {
				t.Fatal(err)
			}
			if len(data) != len(contents) {
				t.Fatal(err)
			}
		})
	}

}

func TestStorageCreateDir(t *testing.T) {
	fn := newFn()
	for id, s := range storages {
		t.Run(id, func(t *testing.T) {
			s.Delete(ctx, fn)
			if err := s.CreateDir(ctx, fn); err != nil {
				t.Fatal(err)
			}

			md, _ := s.GetMD(ctx, fn)
			if md.Path != fn {
				t.Fatal()
			}
			if !md.IsDir {
				t.Fatal()
			}
		})
	}
}

func TestStorageDelete(t *testing.T) {
	fn := newFn()
	for id, s := range storages {
		t.Run(id, func(t *testing.T) {
			s.CreateDir(ctx, fn)
			md, _ := s.GetMD(ctx, fn)
			if md.Path != fn {
				t.Fatal()
			}
			if err := s.Delete(ctx, fn); err != nil {
				t.Fatal(err)
			}
			md, err := s.GetMD(ctx, fn)
			if err == nil {
				t.Fatal(err)
			}
		})
	}
}
