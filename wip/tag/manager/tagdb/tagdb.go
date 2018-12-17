package tagdb

import (
	"context"
	"database/sql"
	"fmt"
	"path"
	"strings"

	"github.com/cernbox/reva/pkg/storage"
	"github.com/cernbox/reva/pkg/tag"
	"github.com/cernbox/reva/pkg/user"

	_ "github.com/go-sql-driver/mysql" // import mysql driver
	"github.com/pkg/errors"
)

const versionPrefix = ".sys.v#."

type tagManager struct {
	db                                     *sql.DB
	dbUsername, dbPassword, dbHost, dbName string
	dbPort                                 int
}

func (tm *tagManager) getDB() (*sql.DB, error) {
	if tm.db != nil {
		return tm.db, nil
	}

	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", tm.dbUsername, tm.dbPassword, tm.dbHost, tm.dbPort, tm.dbName))
	if err != nil {
		return nil, errors.Wrap(err, "tagdb: error connecting to db")

	}

	tm.db = db
	return tm.db, nil
}

// New returns a new tag manager that connects to a mysql database for persistency.
func New(dbUsername, dbPassword, dbHost string, dbPort int, dbName string) tag.Manager {
	return &tagManager{
		dbUsername: dbUsername,
		dbPassword: dbPassword,
		dbName:     dbName,
		dbHost:     dbHost,
		dbPort:     dbPort,
	}
}

func (tm *tagManager) getDBTag(ctx context.Context, u *user.User, prefix, fileID, key string) (*dbTag, error) {
	db, err := tm.getDB()
	if err != nil {
		return nil, errors.Wrap(err, "tagdb: error getting db")
	}

	var (
		id       int64
		itemType int
		value    string
	)

	query := "select id,item_type,coalesce(tag_val, '') as tag_val from cbox_metadata where uid=? and fileid_prefix=? and fileid=? and tag_key=?"
	if err := db.QueryRow(query, u.Account, prefix, fileID, key).Scan(&id, &itemType, &value); err != nil {
		if err == sql.ErrNoRows {
			err := tagNotFoundError(key)
			return nil, errors.Wrap(err, "tagdb: tag not found")
		}
		return nil, errors.Wrapf(err, "tagdb: error retrieving tag with key=%s", key)
	}

	t := &dbTag{
		id:       id,
		itemType: itemType,
		value:    value,
		key:      key,
		uid:      u.Account,
		prefix:   prefix,
		fileID:   fileID,
	}

	return t, nil

}

func (tm *tagManager) SetTag(ctx context.Context, u *user.User, key, val string, md *storage.MD) error {
	db, err := tm.getDB()
	if err != nil {
		return errors.Wrap(err, "tagdb: error getting db")
	}

	var fileID string
	if md.MigId != "" {
		fileID = md.MigId
	} else {
		fileID = md.ID
	}

	prefix, fileID := splitFileID(fileID)

	var itemType = 0 // directory
	if !md.IsDir {
		itemType = 1
	}

	// if tag exists, we don't create a new one
	if _, err := tm.getDBTag(ctx, u, prefix, fileID, key); err == nil {
		return nil
	}

	stmtString := "insert into cbox_metadata set item_type=?,uid=?,fileid_prefix=?,fileid=?,tag_key=?,tag_val=?"
	stmtValues := []interface{}{itemType, u.Account, prefix, fileID, key, val}

	stmt, err := db.Prepare(stmtString)
	if err != nil {
		return errors.Wrapf(err, "tagdb: error preparing statement=%s", stmtString)
	}

	result, err := stmt.Exec(stmtValues...)
	if err != nil {
		return errors.Wrapf(err, "tagdb: error executing statement=%s with values=%v", stmtString, stmtValues)
	}

	_, err = result.LastInsertId()
	if err != nil {
		return errors.Wrapf(err, "tagdb: error retrieving last id")
	}

	return nil
}

/*
func (tm *tagManager) SetTag(ctx context.Context, u *user.User, key, val, md *storage.MD) error {
	md, err := tm.vfs.GetMetadata(ctx, path)
	if err != nil {
		l.Error("error getting md for path", zap.String("path", path), zap.Error(err))
		return err
	}

	var fileID string
	if md.MigId != "" {
		fileID = md.MigId
	} else {
		fileID = md.Id
	}

	prefix, fileID := splitFileID(fileID)

	var itemType tag.Tag_ItemType
	if md.IsDir {
		itemType = tag.Tag_FOLDER
	} else {
		itemType = tag.Tag_FILE
		// if link points to a file we need to use the versions folder inode.
		versionFolderID, err := tm.getVersionFolderID(ctx, md.Path)
		_, fileID = splitFileID(versionFolderID)
		if err != nil {
			l.Error("error getting versions folder for file", zap.Error(err))
			return err
		}
	}

	// if tag exists, we don't create a new one
	if _, err := tm.getDBTag(ctx, u.Account, prefix, fileID, key); err == nil {
		l.Info("aborting creation of new tag, as tag already exists")
		return nil
	}

	stmtString := "insert into cbox_metadata set item_type=?,uid=?,fileid_prefix=?,fileid=?,tag_key=?,tag_val=?"
	stmtValues := []interface{}{itemType, u.Account, prefix, fileID, key, val}

	stmt, err := db.Prepare(stmtString)
	if err != nil {
		l.Error("error preparing stmt", zap.Error(err))
		return err
	}

	result, err := stmt.Exec(stmtValues...)
	if err != nil {
		l.Error("error executing stmt", zap.Error(err))
		return err
	}

	lastId, err := result.LastInsertId()
	if err != nil {
		l.Error("error getting db id", zap.Error(err))
		return err
	}

	l.Info("tag inserted", zap.Int64("id", lastId), zap.String("key", key), zap.String("val", val), zap.String("uid", u.Account))
	return nil
}
*/

func (tm *tagManager) UnSetTag(ctx context.Context, u *user.User, key, val string, md *storage.MD) error {
	db, err := tm.getDB()
	if err != nil {
		return errors.Wrap(err, "tagdb: error getting db")
	}

	var fileID string
	if md.MigId != "" {
		fileID = md.MigId
	} else {
		fileID = md.ID
	}

	prefix, fileID := splitFileID(fileID)
	/* TODO refactor outside
	if !md.IsDir {
		versionFolderID, err := tm.getVersionFolderID(ctx, md.Path)
		_, fileID = splitFileID(versionFolderID)
		if err != nil {
			l.Error("error getting versions folder for file", zap.Error(err))
			return err
		}
	}
	*/

	stmtString := "delete from cbox_metadata where uid=? and fileid_prefix=? and fileid=? and tag_key=?"
	stmtValues := []interface{}{u.Account, prefix, fileID, key}
	stmt, err := db.Prepare(stmtString)
	if err != nil {
		return errors.Wrapf(err, "tagdb: error preparing statement=%s", stmtString)
	}

	res, err := stmt.Exec(stmtValues)
	if err != nil {
		return errors.Wrapf(err, "tagdb: error executing statement=%s with values=%v for removing tag", stmtString, stmtValues)
	}

	_, err = res.RowsAffected()
	if err != nil {
		return errors.Wrap(err, "tagdb: error while getting affected rows")
	}

	return nil
}

type dbTag struct {
	id       int64
	itemType int
	uid      string
	prefix   string
	fileID   string
	key      string
	value    string
}

func (tm *tagManager) convertToTag(dbTag *dbTag) *tag.Tag {
	var tagType tag.Type
	if dbTag.itemType == 0 {
		tagType = tag.TypeDirectory
	} else {
		tagType = tag.TypeFile
	}

	fn := joinFileID(dbTag.prefix, dbTag.fileID)

	t := &tag.Tag{
		ID:       dbTag.id,
		Type:     tagType,
		Filename: fn,
		Key:      dbTag.key,
		Value:    dbTag.value,
		Owner:    dbTag.uid,
	}

	return t
}

func (tm *tagManager) GetTagsForKey(ctx context.Context, u *user.User, key string) ([]*tag.Tag, error) {
	db, err := tm.getDB()
	if err != nil {
		return nil, errors.Wrap(err, "tagdb: error getting db")
	}

	query := "select id, item_type, fileid_prefix, fileid, coalesce(tag_val, '') as tag_val from cbox_metadata where uid=? and tag_key=?"
	rows, err := db.Query(query, u.Account, key)
	if err != nil {
		return nil, errors.Wrapf(err, "tagdb: error getting tags for key=%s", key)
	}

	defer rows.Close()

	var (
		id           int64
		itemType     int
		fileIDPrefix string
		fileID       string
		value        string
	)

	tags := []*tag.Tag{}
	for rows.Next() {
		err := rows.Scan(&id, &itemType, &fileIDPrefix, &fileID, &value)
		if err != nil {
			return nil, err
		}

		dbTag := &dbTag{id: id, itemType: itemType, uid: u.Account, prefix: fileIDPrefix, fileID: fileID, key: key, value: value}
		tag := tm.convertToTag(dbTag)

		/* TODO refactor outside
		if tag.ItemType == tag.Tag_FILE {
			fileID = joinFileID(tag.FileIdPrefix, tag.FileId)
			md, err := tm.vfs.GetMetadata(ctx, fileID)
			if err != nil {
				// TOOD(labkode): log wan here
				continue
			}

			versionFolder := md.Path
			filename := getFileIDFromVersionFolder(versionFolder)

			md, err = tm.vfs.GetMetadata(ctx, filename)
			if err != nil {
				// TOOD(labkode): log wan here
				continue
			}
			_, id := splitFileID(md.Id)
			tag.FileId = id
		}
		*/

		tags = append(tags, tag)
	}

	err = rows.Err()
	if err != nil {
		return nil, errors.Wrap(err, "tagdb: error getting tags")
	}

	return tags, nil
}

func getFileIDFromVersionFolder(p string) string {
	basename := path.Base(p)
	basename = strings.TrimPrefix(basename, "/")
	basename = strings.TrimPrefix(basename, versionPrefix)
	filename := path.Join(path.Dir(p), basename)
	return filename
}

func getVersionFolder(p string) string {
	basename := path.Base(p)
	versionFolder := path.Join(path.Dir(p), versionPrefix+basename)
	return versionFolder
}

// getFileIDParts returns the two parts of a fileID.
// A fileID like home:1234 will be separated into the prefix (home) and the inode(1234).
func splitFileID(fileID string) (string, string) {
	tokens := strings.Split(fileID, ":")
	return tokens[0], tokens[1]
}

// joinFileID concatenates the prefix and the inode to form a valid fileID.
func joinFileID(prefix, inode string) string {
	return strings.Join([]string{prefix, inode}, ":")
}

/*
func (tm *tagManager) getVersionFolderID(ctx context.Context, p string) (string, error) {
	versionFolder := getVersionFolder(p)
	md, err := tm.vfs.GetMetadata(ctx, versionFolder)
	if err != nil {
		if err := tm.vfs.CreateDir(ctx, versionFolder); err != nil {
			return "", err
		}
		md, err = tm.vfs.GetMetadata(ctx, versionFolder)
		if err != nil {
			return "", err
		}
	}
	return md.Id, nil
}
*/

type tagNotFoundError string

func (e tagNotFoundError) Error() string { return string(e) }
