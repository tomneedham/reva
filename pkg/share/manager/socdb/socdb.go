package ssocdb

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/cernbox/reva/pkg/share"
	"github.com/cernbox/reva/pkg/storage"
	"github.com/cernbox/reva/pkg/user"

	_ "github.com/go-sql-driver/mysql"
	"github.com/pkg/errors"
)

func New(dbUsername, dbPassword, dbHost string, dbPort int, dbName string) (share.ShareManager, error) {
	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", dbUsername, dbPassword, dbHost, dbPort, dbName))
	if err != nil {
		return nil, err
	}

	return &shareManager{db: db}, nil
}

type shareManager struct {
	db     *sql.DB
	logger *logger
}

func (sm *shareManager) RejectReceivedShare(ctx context.Context, u *user.User, id string) error {
	err := sm.rejectShare(ctx, u, id)
	if err != nil {
		err = errors.Wrapf(err, "error rejecting db share: id=%s user=%s", id, u.Account)
		return err
	}

	return nil
}

func (sm *shareManager) rejectShare(ctx context.Context, u *user.User, id string) error {
	intID, err := strconv.ParseInt(id, 10, 64)
	if err != nil {
		err = errors.Wrapf(err, "cannot parse id to int64: id=%s", id)
		return err
	}

	_, err = sm.getDBShareWithMe(ctx, u, id)
	if err != nil {
		err = errors.Wrapf(err, "error getting share: id=%s user=%s", id, u)
		return err
	}

	query := "insert into oc_share_acl(id, rejected_by) values(?, ?)"
	stmt, err := sm.db.Prepare(query)
	if err != nil {
		err = errors.Wrapf(err, "error preparing statement: id=%s", id)
		return err
	}

	_, err = stmt.Exec(intID, u)
	if err != nil {
		err = errors.Wrapf(err, "error updating db: id=%s", id)
		return err
	}
	return nil
}

func (sm *shareManager) GetReceivedShare(ctx context.Context, u *user.User, id string) (*share.Share, error) {
	dbShare, err := sm.getDBShareWithMe(ctx, u, id)
	if err != nil {
		return nil, errors.Wrapf(err, "socdb: error retrieving db share %s for user %s", id, u.Account)
	}

	share, err := sm.convertToReceivedShare(ctx, dbShare)
	if err != nil {
		return nil, errors.Wrap(err, "error converting dbshare")
	}
	return share, nil
}
func (sm *shareManager) ListReceivedShares(ctx context.Context, u *user.User) ([]*share.Share, error) {
	dbShares, err := sm.getDBSharesWithMe(ctx, u)
	if err != nil {
		return nil, err
	}
	shares := []*share.Share{}
	for _, dbShare := range dbShares {
		share, err := sm.convertToReceivedShare(ctx, dbShare)
		if err != nil {
			err := errors.Wrap(err, "socdb: error converting share")
			sm.logger.log(ctx, err.Error())
			continue
		}
		shares = append(shares, share)

	}
	return shares, nil

}

func (sm *shareManager) ListShares(ctx context.Context, u *user.User, md *storage.MD) ([]*share.Share, error) {
	var id string
	if md != nil {
		if md.MigId != "" {
			id = md.MigId
		} else {
			id = md.ID
		}
	}

	dbShares, err := sm.getDBShares(ctx, u.Account, id)
	if err != nil {
		return nil, err
	}
	shares := []*share.Share{}
	for _, dbShare := range dbShares {
		share, err := sm.convertToShare(ctx, dbShare)
		if err != nil {
			err := errors.Wrap(err, "socdb: error converting dbshare to share")
			sm.logger.log(ctx, err.Error())
			continue
		}
		shares = append(shares, share)

	}
	return shares, nil
}

func (sm *shareManager) UpdateShare(ctx context.Context, u *user.User, id string, mode share.ACLMode) (*share.Share, error) {
	s, err := sm.GetShare(ctx, u, id)
	if err != nil {
		return nil, errors.Wrap(err, "socdb: error geting share")
	}

	stmtString := "update oc_share set "
	stmtPairs := map[string]interface{}{}

	if mode == share.ACLModeReadOnly {
		stmtPairs["permissions"] = 1
	} else {
		stmtPairs["permissions"] = 15
	}

	stmtTail := []string{}
	stmtValues := []interface{}{}

	for k, v := range stmtPairs {
		stmtTail = append(stmtTail, k+"=?")
		stmtValues = append(stmtValues, v)
	}

	stmtString += strings.Join(stmtTail, ",") + " where uid_owner=? and id=?"
	stmtValues = append(stmtValues, u.Account, id)

	stmt, err := sm.db.Prepare(stmtString)
	if err != nil {
		return nil, errors.Wrapf(err, "socdb: error preparing statement %s with values %v", stmtString, stmtValues)
	}

	_, err = stmt.Exec(stmtValues...)
	if err != nil {
		return nil, errors.Wrapf(err, "socdb: error executing statement %s with values %v", stmtString, stmtValues)
	}

	s, err = sm.GetShare(ctx, u, id)
	if err != nil {
		return nil, errors.Wrapf(err, "socdb: error retrieving share with id=%s after being updated", id)
	}

	return s, nil

	//  update acl on the storage
	/* TODO(labkode): refactor outside
	err = sm.vfs.SetACL(ctx, md.Path, share.ReadOnly, share.Recipient, []*share.Share{})
	if err != nil {
		l.Error("error setting acl on storage, rollbacking operation", zap.Error(err))
		err2 := sm.Unshare(ctx, share.Id)
		if err2 != nil {
			l.Error("cannot remove non commited share, fix manually", zap.Error(err2), zap.String("share_id", share.Id))
			return nil, err2
		}
		return nil, err
	}

	l.Info("share commited on storage acl", zap.String("share_id", share.Id))
	return share, nil
	*/
}
func (sm *shareManager) Unshare(ctx context.Context, u *user.User, id string) error {
	_, err := sm.GetShare(ctx, u, id)
	if err != nil {
		return errors.Wrap(err, "socdb: error retrieving share")
	}

	stmt, err := sm.db.Prepare("delete from oc_share where uid_owner=? and id=?")
	if err != nil {
		return errors.Wrap(err, "socdb: error preparing statement")
	}

	res, err := stmt.Exec(u.Account, id)
	if err != nil {
		return errors.Wrap(err, "socdb: error executing statement")
	}

	rowCnt, err := res.RowsAffected()
	if err != nil {
		return errors.Wrap(err, "socdb: error retrieving affected rows")
	}

	if rowCnt == 0 {
		err := shareNotFoundError(id)
		return errors.Wrap(err, "socdb: share not found")
	}

	return nil

	// re-set acl on the storage
	/* TODO: refactor this outside
	err = sm.vfs.UnsetACL(ctx, share.Path, share.Recipient, []*share.Share{})
	if err != nil {
		l.Error("error removing acl on storage, fix manually", zap.Error(err))
		return err
	}

	l.Info("share removed from storage acl", zap.String("share_id", share.Id))

	return nil
	*/
}

func (sm *shareManager) GetShare(ctx context.Context, u *user.User, id string) (*share.Share, error) {
	dbShare, err := sm.getDBShare(ctx, u, id)
	if err != nil {
		return nil, errors.Wrapf(err, "socdb: error retrieving share with id=%s", id)
	}

	share, err := sm.convertToShare(ctx, dbShare)
	if err != nil {
		return nil, errors.Wrap(err, "socdb: error converting dbshare to share")
	}

	return share, nil
}

func (sm *shareManager) Share(ctx context.Context, u *user.User, md *storage.MD, a *share.ACL) (*share.Share, error) {
	if !md.IsDir {
		err := notSupportedError("share not supported on files")
		return nil, errors.Wrap(err, "")
	}

	itemType := "folder"
	permissions := 1
	if a.Mode == share.ACLModeReadWrite {
		permissions = 15
	}

	var prefix string
	var itemSource string
	if md.MigId != "" {
		prefix, itemSource = splitFileID(md.MigId)
	} else {
		prefix, itemSource = splitFileID(md.ID)
	}

	fileSource, err := strconv.ParseUint(itemSource, 10, 64)
	if err != nil {
		return nil, errors.Wrapf(err, "socdb: error parsing itemSource=%s to int", itemSource)
	}

	shareType := 0 // share.ACLTypeUser
	if a.Type == share.ACLTypeGroup {
		shareType = 1

	}
	targetPath := path.Join("/", path.Base(md.Path))

	stmtString := "insert into oc_share set share_type=?,uid_owner=?,uid_initiator=?,item_type=?,fileid_prefix=?,item_source=?,file_source=?,permissions=?,stime=?,share_with=?,file_target=?"
	stmtValues := []interface{}{shareType, u.Account, u.Account, itemType, prefix, itemSource, fileSource, permissions, time.Now().Unix(), a.Target, targetPath}

	stmt, err := sm.db.Prepare(stmtString)
	if err != nil {
		return nil, errors.Wrapf(err, "socdb: error preparing statement=%s", stmtString)
	}

	result, err := stmt.Exec(stmtValues...)
	if err != nil {
		return nil, errors.Wrapf(err, "socdb: error executing statement=%s with values=%v", stmtString, stmtValues)
	}

	lastId, err := result.LastInsertId()
	if err != nil {
		return nil, errors.Wrap(err, "socdb: error retrieving last id")
	}

	share, err := sm.GetShare(ctx, u, fmt.Sprintf("%d", lastId))
	if err != nil {
		return nil, errors.Wrap(err, "socdb: error retrieving share")
	}

	return share, nil

	// set acl on the storage
	/* TODO refactor this outside
	err = sm.vfs.SetACL(ctx, p, readOnly, recipient, []*share.Share{})
	if err != nil {
		l.Error("error setting acl on storage, rollbacking operation", zap.Error(err))
		err2 := sm.Unshare(ctx, share.Id)
		if err2 != nil {
			l.Error("cannot remove non commited share, fix manually", zap.Error(err2), zap.String("share_id", share.Id))
			return nil, err2
		}
		return nil, err
	}

	l.Info("share commited on storage acl", zap.String("share_id", share.Id))

	return share, nil
	*/
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
	UIDOwner    string
	Prefix      string
	ItemSource  string
	ShareWith   string
	Permissions int
	ShareType   int
	STime       int
	FileTarget  string
	State       int
}

func (sm *shareManager) getDBShareWithMe(ctx context.Context, u *user.User, id string) (*dbShare, error) {
	intID, err := strconv.ParseInt(id, 10, 64)
	if err != nil {
		return nil, errors.Wrapf(err, "socdb: error parsing id=%s to int64", id)
	}

	var (
		uidOwner    string
		shareWith   string
		prefix      string
		itemSource  string
		shareType   int
		stime       int
		permissions int
		fileTarget  string
		state       int
	)

	queryArgs := []interface{}{id, u.Account}
	groupArgs := []interface{}{}
	for _, v := range u.Groups {
		groupArgs = append(groupArgs, v)
	}

	var query string

	if len(u.Groups) > 1 {
		query = "select coalesce(uid_owner, '') as uid_owner, coalesce(share_with, '') as share_with, coalesce(fileid_prefix, '') as fileid_prefix, coalesce(item_source, '') as item_source, stime, permissions, share_type, file_target, accepted from oc_share where id=? and (accepted=0 or accepted=1) and (share_with=? or share_with in (?" + strings.Repeat(",?", len(u.Groups)-1) + ")) and id not in (select distinct(id) from oc_share_acl where rejected_by=?)"
		queryArgs = append(queryArgs, groupArgs...)
		queryArgs = append(queryArgs, u.Account)
	} else {
		query = "select coalesce(uid_owner, '') as uid_owner, coalesce(share_with, '') as share_with, coalesce(fileid_prefix, '') as fileid_prefix, coalesce(item_source, '') as item_source, stime, permissions, share_type, file_target, accepted from oc_share where id=? and (accepted=0 or accepted=1) and (share_with=?) and id not in (select distinct(id) from oc_share_acl where rejected_by=?)"
		queryArgs = append(queryArgs, u.Account)
	}

	if err := sm.db.QueryRow(query, queryArgs...).Scan(&uidOwner, &shareWith, &prefix, &itemSource, &stime, &permissions, &shareType, &fileTarget, &state); err != nil {
		if err == sql.ErrNoRows {
			err = shareNotFoundError(id)
			return nil, errors.Wrap(err, "socdb: error retrieving share")
		}
		return nil, err
	}
	dbShare := &dbShare{ID: int(intID), UIDOwner: uidOwner, Prefix: prefix, ItemSource: itemSource, ShareWith: shareWith, STime: stime, Permissions: permissions, ShareType: shareType, FileTarget: fileTarget, State: state}
	return dbShare, nil

}

func (sm *shareManager) getDBSharesWithMe(ctx context.Context, u *user.User) ([]*dbShare, error) {
	queryArgs := []interface{}{0, 1, u.Account, u.Account}
	groupArgs := []interface{}{}
	for _, v := range u.Groups {
		groupArgs = append(groupArgs, v)
	}

	var query string

	if len(u.Groups) > 1 {
		query = "select id, coalesce(uid_owner, '') as uid_owner, coalesce(share_with, '') as share_with, coalesce(fileid_prefix, '') as fileid_prefix, coalesce(item_source, '') as item_source, stime, permissions, share_type, file_target from oc_share where (accepted=0 or accepted=1) and (share_type=? or share_type=?) and uid_owner!=? and (share_with=? or share_with in (?" + strings.Repeat(",?", len(u.Groups)-1) + ")) and id not in (select distinct(id) from oc_share_acl where rejected_by=?)"
		queryArgs = append(queryArgs, groupArgs...)
		queryArgs = append(queryArgs, u.Account)
	} else {
		query = "select id, coalesce(uid_owner, '') as uid_owner, coalesce(share_with, '') as share_with, coalesce(fileid_prefix, '') as fileid_prefix, coalesce(item_source, '') as item_source, stime, permissions, share_type, file_target from oc_share where (accepted=0 or accepted=1) and (share_type=? or share_type=?) and uid_owner!=? and (share_with=?) and id not in (select distinct(id) from oc_share_acl where rejected_by=?)"
		queryArgs = append(queryArgs, u.Account)
	}
	rows, err := sm.db.Query(query, queryArgs...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var (
		id          int
		uidOwner    string
		shareWith   string
		prefix      string
		itemSource  string
		shareType   int
		stime       int
		permissions int
		fileTarget  string
	)

	dbShares := []*dbShare{}
	for rows.Next() {
		err := rows.Scan(&id, &uidOwner, &shareWith, &prefix, &itemSource, &stime, &permissions, &shareType, &fileTarget)
		if err != nil {
			return nil, err
		}
		dbShare := &dbShare{ID: id, UIDOwner: uidOwner, Prefix: prefix, ItemSource: itemSource, ShareWith: shareWith, STime: stime, Permissions: permissions, ShareType: shareType, FileTarget: fileTarget}
		dbShares = append(dbShares, dbShare)

	}
	err = rows.Err()
	if err != nil {
		return nil, err
	}

	return dbShares, nil
}

func (sm *shareManager) getDBShare(ctx context.Context, u *user.User, id string) (*dbShare, error) {
	intID, err := strconv.ParseInt(id, 10, 64)
	if err != nil {
		return nil, errors.Wrapf(err, "socdb: error parsing id=%s to int64", id)
	}

	var (
		uidOwner    string
		shareWith   string
		prefix      string
		itemSource  string
		shareType   int
		stime       int
		permissions int
	)

	query := "select coalesce(uid_owner, '') as uid_owner, coalesce(share_with, '') as share_with, coalesce(fileid_prefix, '') as fileid_prefix, coalesce(item_source, '') as item_source, stime, permissions, share_type from oc_share where uid_owner=? and id=?"
	if err := sm.db.QueryRow(query, u.Account, id).Scan(&uidOwner, &shareWith, &prefix, &itemSource, &stime, &permissions, &shareType); err != nil {
		if err == sql.ErrNoRows {
			err := shareNotFoundError(id)
			return nil, errors.Wrap(err, "socdb: share not found")
		}
		return nil, err
	}
	dbShare := &dbShare{ID: int(intID), UIDOwner: uidOwner, Prefix: prefix, ItemSource: itemSource, ShareWith: shareWith, STime: stime, Permissions: permissions, ShareType: shareType}
	return dbShare, nil

}

func (sm *shareManager) getDBShares(ctx context.Context, accountID, filterByFileID string) ([]*dbShare, error) {
	query := "select id, coalesce(uid_owner, '') as uid_owner,  coalesce(share_with, '') as share_with, coalesce(fileid_prefix, '') as fileid_prefix, coalesce(item_source, '') as item_source, stime, permissions, share_type from oc_share where uid_owner=? and (share_type=? or share_type=?) "
	params := []interface{}{accountID, 0, 1}
	if filterByFileID != "" {
		prefix, itemSource := splitFileID(filterByFileID)
		query += "and fileid_prefix=? and item_source=?"
		params = append(params, prefix, itemSource)
	}

	rows, err := sm.db.Query(query, params...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var (
		id          int
		uidOwner    string
		shareWith   string
		prefix      string
		itemSource  string
		shareType   int
		stime       int
		permissions int
	)

	dbShares := []*dbShare{}
	for rows.Next() {
		err := rows.Scan(&id, &uidOwner, &shareWith, &prefix, &itemSource, &stime, &permissions, &shareType)
		if err != nil {
			return nil, err
		}
		dbShare := &dbShare{ID: id, UIDOwner: uidOwner, Prefix: prefix, ItemSource: itemSource, ShareWith: shareWith, STime: stime, Permissions: permissions, ShareType: shareType}
		dbShares = append(dbShares, dbShare)

	}
	err = rows.Err()
	if err != nil {
		return nil, err
	}

	return dbShares, nil
}

func (sm *shareManager) getShareACL(ctx context.Context, dbShare *dbShare) (*share.ACL, error) {
	var aclType share.ACLType
	var aclMode share.ACLMode

	if dbShare.ShareType == 0 {
		aclType = share.ACLTypeUser
	} else if dbShare.ShareType == 1 {
		aclType = share.ACLTypeGroup
	} else {
		return nil, errors.Wrapf(aclTypeNotValidError(""), "socdb: aclType=%d not valid", aclType)
	}

	if dbShare.Permissions == 1 {
		aclMode = share.ACLModeReadOnly
	} else {
		aclMode = share.ACLModeReadWrite
	}

	return &share.ACL{Mode: aclMode, Type: aclType, Target: dbShare.ShareWith}, nil
}

func (sm *shareManager) convertToReceivedShare(ctx context.Context, dbShare *dbShare) (*share.Share, error) {
	acl, err := sm.getShareACL(ctx, dbShare)
	if err != nil {
		return nil, errors.Wrap(err, "socdb: error getting share acl from dbshare")
	}

	path := joinFileID(dbShare.Prefix, dbShare.ItemSource)
	share := &share.Share{
		Owner:    dbShare.UIDOwner,
		ID:       fmt.Sprintf("%d", dbShare.ID),
		Modified: uint64(dbShare.STime),
		Filename: path,
		ACL:      acl,
	}
	return share, nil

}

func (sm *shareManager) convertToShare(ctx context.Context, dbShare *dbShare) (*share.Share, error) {
	acl, err := sm.getShareACL(ctx, dbShare)
	if err != nil {
		return nil, errors.Wrap(err, "socdb: error getting share acl from dbshare")
	}

	path := joinFileID(dbShare.Prefix, dbShare.ItemSource)
	share := &share.Share{
		Owner:    dbShare.UIDOwner,
		ID:       fmt.Sprintf("%d", dbShare.ID),
		Modified: uint64(dbShare.STime),
		Filename: path,
		ACL:      acl,
	}
	return share, nil

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

type logger struct {
	out io.Writer
	key interface{}
}

func (l *logger) log(ctx context.Context, msg string) {
	trace := l.getTraceFromCtx(ctx)
	fmt.Fprintf(l.out, "eosclient: trace=%s %s", trace, msg)
}

func (l *logger) getTraceFromCtx(ctx context.Context) string {
	trace, _ := ctx.Value("traceid").(string)
	if trace == "" {
		trace = "notrace"
	}
	return trace
}

type shareNotFoundError string
type notSupportedError string
type aclTypeNotValidError string

func (e shareNotFoundError) Error() string    { return string(e) }
func (e shareNotFoundError) IsShareNotFound() {}
func (e notSupportedError) Error() string     { return string(e) }
func (e notSupportedError) IsNotSupported()   {}
func (e aclTypeNotValidError) Error() string  { return string(e) }
