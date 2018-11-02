package psocdb

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"math/rand"
	path "path"
	"strconv"
	"strings"
	"time"

	"github.com/cernbox/reva/pkg/publicshare"
	"github.com/cernbox/reva/pkg/storage"
	"github.com/cernbox/reva/pkg/user"

	"github.com/bluele/gcache"
	_ "github.com/go-sql-driver/mysql"
	"github.com/pkg/errors"
	"golang.org/x/crypto/bcrypt"
)

func init() {
	// Seed the random source with unix nano time
	rand.Seed(time.Now().UTC().UnixNano())
}

//TODO(labkode): add owner_id to other public link queries when consulting db
const tokenLength = 15
const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
const versionPrefix = ".sys.v#."

func New(dbUsername, dbPassword, dbHost string, dbPort int, dbName string, cacheSize, cacheEviction int) (publicshare.PublicShareManager, error) {
	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", dbUsername, dbPassword, dbHost, dbPort, dbName))
	if err != nil {
		return nil, err
	}

	cache := gcache.New(cacheSize).LFU().Build()
	return &linkManager{db: db, cache: cache, cacheEviction: time.Second * time.Duration(cacheEviction)}, nil
}

type linkManager struct {
	db            *sql.DB
	cache         gcache.Cache
	cacheSize     int
	cacheEviction time.Duration
	logger        *logger
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
func (lm *linkManager) AuthenticatePublicShare(ctx context.Context, token, password string) (*publicshare.PublicShare, error) {
	l := ctx_zap.Extract(ctx)
	dbShare, err := lm.getDBShareByToken(ctx, token)
	if err != nil {
		l.Error("", zap.Error(err))
		return nil, err
	}
	pb, err := lm.convertToPublicShare(ctx, dbShare)
	if err != nil {
		l.Error("error converting db share to public link", zap.Error(err))
		return nil, err
	}

	// check expiration time
	if pb.Expires != 0 {
		now := time.Now().Unix()
		if uint64(now) > pb.Expires {
			l.Warn("public link has expired", zap.String("id", pb.Id))
			return nil, publicshare.NewError(publicshare.PublicShareInvalidExpireDateErrorCode)

		}
	}

	if pb.Protected {
		hashedPassword := strings.TrimPrefix(dbShare.ShareWith, "1|")
		ok := checkPasswordHash(password, hashedPassword)
		if !ok {
			return nil, publicshare.NewError(publicshare.PublicShareInvalidPasswordErrorCode)
		}
	}

	return pb, nil
}

func (lm *linkManager) IsPublicShareProtected(ctx context.Context, token string) (bool, error) {
	l := ctx_zap.Extract(ctx)
	dbShare, err := lm.getDBShareByToken(ctx, token)
	if err != nil {
		l.Error("", zap.Error(err))
		return false, err
	}
	pb, err := lm.convertToPublicShare(ctx, dbShare)
	if err != nil {
		l.Error("", zap.Error(err))
		return false, err
	}
	return pb.Protected, nil
}
*/

func (lm *linkManager) GetPublicShareByToken(ctx context.Context, token string) (*publicshare.PublicShare, error) {
	dbShare, err := lm.getDBShareByToken(ctx, token)
	if err != nil {
		return nil, errors.Wrapf(err, "psocdb: error getting share from db by token=%s", token)
	}

	pb, err := lm.convertToPublicShare(ctx, dbShare)
	if err != nil {
		return nil, errors.Wrapf(err, "psocdb: error convering db share to public share for token=%s", token)
	}

	return pb, nil

}

/*
func (lm *linkManager) getVersionFolderID(ctx context.Context, p string) (string, error) {
	versionFolder := getVersionFolder(p)
	md, err := lm.vfs.GetMetadata(ctx, versionFolder)
	if err != nil {
		if err := lm.vfs.CreateDir(ctx, versionFolder); err != nil {
			return "", err
		}
		md, err = lm.vfs.GetMetadata(ctx, versionFolder)
		if err != nil {
			return "", err
		}
	}
	return md.Id, nil
}
*/

func (lm *linkManager) CreatePublicShare(ctx context.Context, u *user.User, md *storage.MD, a *publicshare.ACL) (*publicshare.PublicShare, error) {
	var prefix, itemSource string
	if md.MigId != "" {
		prefix, itemSource = splitFileID(md.MigId)
	} else {
		prefix, itemSource = splitFileID(md.ID)
	}

	/* TODO refactor outside
	itemType := "file"
	if md.IsDir {
		itemType = "folder"
	} else {
		// if link points to a file we need to use the versions folder inode.
		if !md.IsDir {
			versionFolderID, err := lm.getVersionFolderID(ctx, md.Path)
			_, itemSource = splitFileID(versionFolderID)
			if err != nil {
				l.Error("", zap.Error(err))
				return nil, err
			}
		}

	}
	*/

	itemType := 0
	if a.Type == publicshare.ACLTypeFile {
		itemType = 1
	}

	permissions := 15
	if a.Mode == publicshare.ACLModeReadOnly {
		permissions = 1
	}

	token := genToken()
	_, err := lm.getDBShareByToken(ctx, token)
	if err == nil { // token already exists, abort
		err := tokenAlreadyExistsError(token)
		return nil, errors.Wrap(err, "psocdb: token already exists")
	}

	if err != nil {
		return nil, errors.Wrap(err, "psocdb: error checking if token already exists")
	}

	fileSource, err := strconv.ParseUint(itemSource, 10, 64)
	if err != nil {
		return nil, errors.Wrap(err, "psocdb: error parsing itemSource to int64")
	}

	shareName := path.Base(md.Path)

	stmtString := "insert into oc_share set share_type=?,uid_owner=?,uid_initiator=?,item_type=?,fileid_prefix=?,item_source=?,file_source=?,permissions=?,stime=?,token=?,share_name=?"
	stmtValues := []interface{}{3, u.Account, u.Account, itemType, prefix, itemSource, fileSource, permissions, time.Now().Unix(), token, shareName}

	if a.Password != "" {
		hashedPassword, err := hashPassword(a.Password)
		if err != nil {
			return nil, err
		}
		hashedPassword = "1|" + hashedPassword
		stmtString += ",share_with=?"
		stmtValues = append(stmtValues, hashedPassword)
	}

	if a.Expiration != 0 {
		t := time.Unix(int64(a.Expiration), 0)
		stmtString += ",expiration=?"
		stmtValues = append(stmtValues, t)
	}

	stmt, err := lm.db.Prepare(stmtString)
	if err != nil {
		return nil, errors.Wrapf(err, "psocdb: error preparing statement=%s", stmt)
	}

	result, err := stmt.Exec(stmtValues...)
	if err != nil {
		return nil, errors.Wrapf(err, "psocdb: error executing statement=%s with values=%+v", stmt, stmtValues)
	}

	lastId, err := result.LastInsertId()
	if err != nil {
		return nil, errors.Wrap(err, "psocdb: error retrieving last inserted id")
	}

	pb, err := lm.GetPublicShare(ctx, u, fmt.Sprintf("%d", lastId))
	if err != nil {
		return nil, errors.Wrapf(err, "psocdb: error getting public share after creating it with id=%s", lastId)
	}

	return pb, nil
}

// TODO(labkode): handle nil opt
func (lm *linkManager) UpdatePublicShare(ctx context.Context, u *user.User, id string, up *publicshare.UpdatePolicy, a *publicshare.ACL) (*publicshare.PublicShare, error) {
	pb, err := lm.GetPublicShare(ctx, u, id)
	if err != nil {
		return nil, errors.Wrapf(err, "psocdb: error getting public share with id=%d", id)
	}

	stmtString := "update oc_share set "
	stmtPairs := map[string]interface{}{}

	if up.SetPassword {
		if a.Password == "" {
			stmtPairs["share_with"] = ""

		} else {
			hashedPassword, err := hashPassword(a.Password)
			if err != nil {
				return nil, err
			}
			hashedPassword = "1|" + hashedPassword
			stmtPairs["share_with"] = hashedPassword
		}
	}

	if up.SetExpiration {
		t := time.Unix(int64(a.Expiration), 0)
		stmtPairs["expiration"] = t
	}

	if up.SetMode {
		if a.Mode == publicshare.ACLModeReadOnly {
			stmtPairs["permissions"] = 1
		} else {
			stmtPairs["permissions"] = 15
		}
	}

	if len(stmtPairs) == 0 { // nothing to update
		return pb, nil
	}

	stmtTail := []string{}
	stmtValues := []interface{}{}

	for k, v := range stmtPairs {
		stmtTail = append(stmtTail, k+"=?")
		stmtValues = append(stmtValues, v)
	}

	stmtString += strings.Join(stmtTail, ",") + " where uid_owner=? and id=?"
	stmtValues = append(stmtValues, u.Account, id)

	stmt, err := lm.db.Prepare(stmtString)
	if err != nil {
		return nil, errors.Wrapf(err, "psocdb: error preparing statement=%s", stmtString)
	}

	_, err = stmt.Exec(stmtValues...)
	if err != nil {
		return nil, errors.Wrapf(err, "psocdb: error executing statement=%s with values=%+v", stmtString, stmtValues)
	}

	pb, err = lm.GetPublicShare(ctx, u, id)
	if err != nil {
		return nil, errors.Wrapf(err, "psocdb: error getting public share with id=%d", id)
	}
	return pb, nil
}

func (lm *linkManager) GetPublicShare(ctx context.Context, u *user.User, id string) (*publicshare.PublicShare, error) {
	dbShare, err := lm.getDBShare(ctx, u.Account, id)
	if err != nil {
		return nil, errors.Wrapf(err, "psocdb: error getting share with id=%d", id)
	}

	pb, err := lm.convertToPublicShare(ctx, dbShare)
	if err != nil {
		return nil, errors.Wrap(err, "psocdb: error converting dbshare to public share")
	}
	return pb, nil
}

func (lm *linkManager) ListPublicShares(ctx context.Context, u *user.User, md *storage.MD) ([]*publicshare.PublicShare, error) {
	var fileID string
	if md != nil {
		if md.MigId != "" {
			fileID = md.MigId
		} else {
			fileID = md.ID
		}
		/*
			if !md.IsDir {
				// conver to version folder
				versionFolder := getVersionFolder(md.Path)
				mdVersion, err := lm.vfs.GetMetadata(ctx, versionFolder)
				if err == nil {
					if mdVersion.MigId != "" {
						fileID = mdVersion.MigId
					} else {
						fileID = mdVersion.Id
					}
				} else {
					// the version folder does not exist, this means that the file is not being shared by public link
					// in that case we use the inode of the files to do the search as it will never be stored in the db.
					fileID = md.Id
				}

			} else {
				if md.MigId != "" {
					fileID = md.MigId
				} else {
					fileID = md.Id
				}
			}
		*/
	}

	dbShares, err := lm.getDBShares(ctx, u.Account, fileID)
	if err != nil {
		return nil, errors.Wrapf(err, "psocdb: error listing public shares for fileID=%s", fileID)
	}

	publicLinks := []*publicshare.PublicShare{}
	for _, dbShare := range dbShares {
		pb, err := lm.convertToPublicShare(ctx, dbShare)
		if err != nil {
			err = errors.Wrapf(err, "psocdb: error converting dbshare with id=%d to public share", dbShare.ID)
			lm.logger.log(ctx, err.Error())
			continue
		}
		publicLinks = append(publicLinks, pb)

	}
	return publicLinks, nil
}

func (lm *linkManager) RevokePublicShare(ctx context.Context, u *user.User, id string) error {
	stmt, err := lm.db.Prepare("delete from oc_share where uid_owner=? and id=?")
	if err != nil {
		return errors.Wrapf(err, "psocdb: error preparing statement=%s", stmt)
	}

	stmtValues := []interface{}{u.Account, id}
	res, err := stmt.Exec(stmtValues)
	if err != nil {
		return errors.Wrapf(err, "psocdb: error executing statement=%s with values=%+v", stmt, stmtValues)
	}

	rowCnt, err := res.RowsAffected()
	if err != nil {
		return errors.Wrap(err, "psocdb: error getting affected rows")
	}

	if rowCnt == 0 {
		err := publicShareNotFoundError(id)
		return errors.Wrapf(err, "psocdb: public share with id=%s not found", id)
	}
	return nil
}

/*
type ocShare struct {
	ID          int64          `db:"id"`
	ShareType   int            `db:"share_type"`
	ShareWith   sql.NullString `db:"share_with"`
	UIDOwner    string         `db:"uid_owner"`
	Parent      sql.NullInt64  `db:"parent"`
	ItemType    sql.NullString `db:"item_type"`
	ItemSource  sql.NullString `db:"item_source"`
	ItemTarget  sql.NullString `db:"item_target"`
	FileSource  sql.NullInt64  `db:"file_source"`
	FileTarget  sql.NullString `db:"file_target"`
	Permissions string         `db:"permissions"`
	STime       int            `db:"stime"`
	Accepted    int            `db:"accepted"`
	Expiration  time.Time      `db:"expiration"`
	Token       sql.NullString `db:"token"`
	MailSend    int            `db:"mail_send"`
}
*/

type dbShare struct {
	ID          int
	Prefix      string
	ItemSource  string
	ShareWith   string
	Token       string
	Expiration  string
	STime       int
	ItemType    string
	Permissions int
	Owner       string
	ShareName   string
}

func (lm *linkManager) getDBShareByToken(ctx context.Context, token string) (*dbShare, error) {
	var (
		id          int
		prefix      string
		itemSource  string
		shareWith   string
		expiration  string
		stime       int
		permissions int
		itemType    string
		uidOwner    string
		shareName   string
	)

	query := "select id, coalesce(share_with, '') as share_with, coalesce(fileid_prefix, '') as fileid_prefix, coalesce(item_source, '') as item_source, coalesce(token,'') as token, coalesce(expiration, '') as expiration, stime, permissions, item_type, uid_owner, coalesce(share_name, '') as share_name from oc_share where share_type=? and token=?"
	if err := lm.db.QueryRow(query, 3, token).Scan(&id, &shareWith, &prefix, &itemSource, &token, &expiration, &stime, &permissions, &itemType, &uidOwner, &shareName); err != nil {
		if err == sql.ErrNoRows {
			err := publicShareNotFoundError(token)
			return nil, errors.Wrap(err, "psocdb: public share not found")
		}

		return nil, errors.Wrapf(err, "psocdb: error getting public share by token=?", token)
	}
	dbShare := &dbShare{ID: id, Prefix: prefix, ItemSource: itemSource, ShareWith: shareWith, Token: token, Expiration: expiration, STime: stime, Permissions: permissions, ItemType: itemType, Owner: uidOwner, ShareName: shareName}
	return dbShare, nil

}

func (lm *linkManager) getDBShare(ctx context.Context, accountID, id string) (*dbShare, error) {
	intID, err := strconv.ParseInt(id, 10, 64)
	if err != nil {
		return nil, errors.Wrapf(err, "psocdb: error parsing id=%s to int64", id)
	}

	var (
		prefix      string
		itemSource  string
		shareWith   string
		expiration  string
		stime       int
		permissions int
		itemType    string
		token       string
		shareName   string
	)

	query := "select coalesce(share_with, '') as share_with, coalesce(fileid_prefix, '') as fileid_prefix, coalesce(item_source, '') as item_source, coalesce(token,'') as token, coalesce(expiration, '') as expiration, stime, permissions, item_type, coalesce(share_name, '') as share_name from oc_share where share_type=? and uid_owner=? and id=?"
	if err := lm.db.QueryRow(query, 3, accountID, id).Scan(&shareWith, &prefix, &itemSource, &token, &expiration, &stime, &permissions, &itemType, &shareName); err != nil {
		if err == sql.ErrNoRows {
			err := publicShareNotFoundError(id)
			return nil, errors.Wrapf(err, "psocdb: public share with id=%s not found")
		}

		return nil, err
	}
	dbShare := &dbShare{ID: int(intID), Prefix: prefix, ItemSource: itemSource, ShareWith: shareWith, Token: token, Expiration: expiration, STime: stime, Permissions: permissions, ItemType: itemType, Owner: accountID, ShareName: shareName}
	return dbShare, nil

}
func (lm *linkManager) getDBShares(ctx context.Context, accountID, fileID string) ([]*dbShare, error) {
	query := "select id, coalesce(share_with, '') as share_with, coalesce(fileid_prefix, '') as fileid_prefix, coalesce(item_source, '') as item_source, coalesce(token,'') as token, coalesce(expiration, '') as expiration, stime, permissions, item_type, coalesce(share_name, '') as share_name from oc_share where share_type=? and uid_owner=? "
	params := []interface{}{3, accountID}

	if fileID != "" {
		prefix, itemSource := splitFileID(fileID)
		query += "and fileid_prefix=? and item_source=?"
		params = append(params, prefix, itemSource)
	}

	fmt.Println("hugo", query, params)

	rows, err := lm.db.Query(query, params...)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	var (
		id          int
		prefix      string
		itemSource  string
		shareWith   string
		token       string
		expiration  string
		stime       int
		permissions int
		itemType    string
		shareName   string
	)

	dbShares := []*dbShare{}
	for rows.Next() {
		err := rows.Scan(&id, &shareWith, &prefix, &itemSource, &token, &expiration, &stime, &permissions, &itemType, &shareName)
		if err != nil {
			return nil, err
		}
		dbShare := &dbShare{ID: id, Prefix: prefix, ItemSource: itemSource, ShareWith: shareWith, Token: token, Expiration: expiration, STime: stime, Permissions: permissions, ItemType: itemType, Owner: accountID, ShareName: shareName}
		dbShares = append(dbShares, dbShare)

	}
	err = rows.Err()
	if err != nil {
		return nil, err
	}

	return dbShares, nil
}

// converToPublicShare converts  an entry from the db to a public link. It the share is to a file, we need
// to convert the version folder back to a file id, hence performing a md operation. This operation is expensive
// but we can perform aggresive caching as it a file exists the version folder will exist and viceversa.
func (lm *linkManager) convertToPublicShare(ctx context.Context, dbShare *dbShare) (*publicshare.PublicShare, error) {
	var expires uint64
	if dbShare.Expiration != "" {
		t, err := time.Parse("2006-01-02 03:04:05", dbShare.Expiration)
		if err != nil {
			return nil, err
		}
		expires = uint64(t.Unix())
	}

	fileID := joinFileID(dbShare.Prefix, dbShare.ItemSource)

	var aclType publicshare.ACLType = publicshare.ACLTypeFile
	if dbShare.ItemType == "folder" {
		aclType = publicshare.ACLTypeFile
	}

	aclMode := publicshare.ACLModeReadOnly
	if dbShare.Permissions > 1 {
		aclMode = publicshare.ACLModeReadWrite
	}

	/* TODO refactor outside
	itemType = publicshare.PublicShare_FILE
	// the share points to the version folder id, we
	// need to point to the file id, so in the UI the share info
	// appears on the latest file version.
	newCtx := publicshare.ContextSetUser(ctx, &publicshare.User{Account: dbShare.Owner})
	//md, err := lm.vfs.GetMetadata(newCtx, fileID)
	md, err := lm.getCachedMetadata(newCtx, fileID)
	if err != nil {
		fmt.Println("hugo", err, fileID)
		l := ctx_zap.Extract(ctx)
		l.Error("error getting metadata for public link", zap.Error(err))
		return nil, err
	}

	versionFolder := md.Path
	filename := getFileIDFromVersionFolder(versionFolder)

	// we cannot cache the call to get metadata of the current version of the file
	// as if we cache it, we will hit the problem that after a public link share is created,
	// the file gets updated, and the cached metadata still points to the old version, with a different
	// file ID
	//md, err = lm.getCachedMetadata(newCtx, filename)
	md, err = lm.vfs.GetMetadata(newCtx, filename)
	if err != nil {
		fmt.Println("hugo", err, fileID)
		return nil, err
	}
	_, id := splitFileID(md.Id)
	fileID = joinFileID(dbShare.Prefix, id)
	*/

	a := &publicshare.ACL{
		Expiration: expires,
		Password:   "",
		Type:       aclType,
		Mode:       aclMode,
	}

	ps := &publicshare.PublicShare{
		ID:          fmt.Sprintf("%d", dbShare.ID),
		Token:       dbShare.Token,
		Modified:    uint64(dbShare.STime),
		ACL:         a,
		Filename:    fileID,
		Owner:       dbShare.Owner,
		DisplayName: dbShare.ShareName,
	}

	return ps, nil

}

/* TODO refactor outside
func (lm *linkManager) getCachedMetadata(ctx context.Context, key string) (*storage.MD, error) {
		//v, err := lm.cache.Get(key)
	//	if err == nil {
	//		if md, ok := v.(*publicshare.Metadata); ok {
	//			l.Debug("revad: publicshare: getCachedMetadata:  md found in cache", zap.String("path", key))
	//			return md, nil
	//		}
	//	}
	//

	md, err := lm.vfs.GetMetadata(ctx, key)
	if err != nil {
		return nil, err
	}
	lm.cache.SetWithExpire(key, md, lm.cacheEviction)
	l.Debug("revad: publicshare: getCachedMetadata: md retrieved and stored  in cache", zap.String("path", key))
	return md, nil
}
*/

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

func genToken() string {
	b := make([]byte, tokenLength)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}

func hashPassword(password string) (string, error) {
	bytes, err := bcrypt.GenerateFromPassword([]byte(password), 14)
	return string(bytes), err
}

func checkPasswordHash(password, hash string) bool {
	err := bcrypt.CompareHashAndPassword([]byte(hash), []byte(password))
	return err == nil
}

type tokenAlreadyExistsError string
type publicShareNotFoundError string

func (e tokenAlreadyExistsError) Error() string  { return string(e) }
func (e publicShareNotFoundError) Error() string { return string(e) }

type logger struct {
	out io.Writer
	key interface{}
}

func (l *logger) log(ctx context.Context, msg string) {
	trace := l.getTraceFromCtx(ctx)
	fmt.Fprintf(l.out, "eosclient: trace=%s %s", trace, msg)
}

func (l *logger) getTraceFromCtx(ctx context.Context) string {
	trace, _ := ctx.Value(l.key).(string)
	if trace == "" {
		trace = "notrace"
	}
	return trace
}
