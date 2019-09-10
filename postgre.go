// Copyright 2019 Fabian Wenzelmann
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package gopherbouncepostgres

import (
	"database/sql"
	"fmt"
	"github.com/FabianWe/gopherbouncedb"
	"github.com/lib/pq"
	"reflect"
	"strconv"
	"strings"
	"time"
)

var (
	DefaultPGRowNames = gopherbouncedb.DefaultUserRowNames
)

const (
	// PGKeyExists is the error code from the pq driver that states
	// that a key error occurred.
	PGKeyExists = "23505"
)


type PGBridge struct{}

func NewPGBridge() PGBridge {
	return PGBridge{}
}

func (b PGBridge) TimeScanType() interface{} {
	var res time.Time
	return &res
}

func (b PGBridge) ConvertTimeScanType(val interface{}) (time.Time, error) {
	switch v := val.(type) {
	case *time.Time:
		return *v, nil
	case time.Time:
		return v, nil
	default:
		var zeroT time.Time
		return zeroT, fmt.Errorf("PGBridge.ConvertTimeScanType: Expected value of *time.Time, got %v",
			reflect.TypeOf(val))
	}
}

func (b PGBridge) ConvertTime(t time.Time) interface{} {
	return t
}

func (b PGBridge) IsDuplicateInsert(err error) bool {
	if postgreErr, ok := err.(*pq.Error); ok && postgreErr.Code == PGKeyExists {
		return true
	}
	return false
}

func (b PGBridge) IsDuplicateUpdate(err error) bool {
	if postgreErr, ok := err.(*pq.Error); ok && postgreErr.Code == PGKeyExists {
		return true
	}
	return false
}

// PGQueries implements gopherbouncedb.UserSQL with support for Postgres.
type PGQueries struct {
	InitS []string
	GetUserS, GetUserByNameS, GetUserByEmailS, InsertUserS,
	UpdateUserS, DeleteUserS, UpdateFieldsS string
	Replacer *gopherbouncedb.SQLTemplateReplacer
	RowNames map[string]string
}

func DefaultPostgreReplacer() *gopherbouncedb.SQLTemplateReplacer {
	return gopherbouncedb.DefaultSQLReplacer()
}

func NewPGQueries(replaceMapping map[string]string) *PGQueries {
	replacer := DefaultPostgreReplacer()
	if replaceMapping != nil {
		replacer.UpdateDict(replaceMapping)
	}
	res := &PGQueries{}
	res.Replacer = replacer
	res.InitS = append(res.InitS, replacer.Apply(PGUsersInit))
	res.GetUserS = replacer.Apply(PGQueryUserID)
	res.GetUserByNameS = replacer.Apply(PGQueryUsername)
	res.GetUserByEmailS = replacer.Apply(PQQueryEmail)
	res.InsertUserS = replacer.Apply(PGInsertUser)
	res.UpdateUserS = replacer.Apply(PGUpdateUser)
	res.DeleteUserS = replacer.Apply(PGDeleteUser)
	res.UpdateFieldsS = replacer.Apply(PGUpdateUserFields)
	res.RowNames = DefaultPGRowNames
	return res
}

func (q *PGQueries) InitUsers() []string {
	return q.InitS
}

func (q *PGQueries) GetUser() string {
	return q.GetUserS
}

func (q *PGQueries) GetUserByName() string {
	return q.GetUserByNameS
}

func (q *PGQueries) GetUserByEmail() string {
	return q.GetUserByEmailS
}

func (q *PGQueries) InsertUser() string {
	return q.InsertUserS
}

func (q *PGQueries) UpdateUser(fields []string) string {
	if len(fields) == 0 || !q.SupportsUserFields() {
		return q.UpdateUserS
	}
	updates := make([]string, len(fields))
	for i, fieldName := range fields {
		if colName, has := q.RowNames[fieldName]; has {
			updates[i] = colName + "=$" + strconv.Itoa(i+1)
		} else {
			panic(fmt.Sprintf("invalid field name \"%s\": Must be a valid field name of gopherbouncedb.UserModel", fieldName))
		}
	}
	updateStr := strings.Join(updates, ",")
	// replace updateStr and id param num in UpdateFieldS
	// we don't create a replacer, too much overhead for just two entries
	stmt := strings.Replace(q.UpdateFieldsS, "$UPDATE_CONTENT$", updateStr, 1)
	stmt = strings.Replace(stmt, "$ID_PARAM_NUM$", "$"+strconv.Itoa(len(fields)+1), 1)
	return stmt
}

func (q *PGQueries) DeleteUser() string {
	return q.DeleteUserS
}

func (q *PGQueries) SupportsUserFields() bool {
	return q.UpdateFieldsS != ""
}

type PGUserStorage struct {
	*gopherbouncedb.SQLUserStorage
}

func NewPGUserStorage(db *sql.DB, replaceMapping map[string]string) *PGUserStorage {
	queries := NewPGQueries(replaceMapping)
	bridge := NewPGBridge()
	sqlStorage := gopherbouncedb.NewSQLUserStorage(db, queries, bridge)
	res := PGUserStorage{sqlStorage}
	return &res
}

// overwrites because of returning, must use query row
func (s *PGUserStorage) InsertUser(user *gopherbouncedb.UserModel) (gopherbouncedb.UserID, error) {
	user.ID = gopherbouncedb.InvalidUserID
	now := time.Now().UTC()
	var zeroTime time.Time
	zeroTime = zeroTime.UTC()
	// use the bridge conversion for time
	// we do this because the bridge could be changed and doing this direct insert could go wrong
	dateJoined := s.UserBridge.ConvertTime(now)
	lastLogin := s.UserBridge.ConvertTime(zeroTime)
	user.DateJoined = now
	user.LastLogin = zeroTime
	var userID gopherbouncedb.UserID
	err := s.UserDB.QueryRow(s.UserQueries.InsertUser(),
		user.Username, user.Password, user.EMail, user.FirstName,
		user.LastName, user.IsSuperUser, user.IsStaff,
		user.IsActive, dateJoined, lastLogin).Scan(&userID)
	if err != nil {
		user.ID = gopherbouncedb.InvalidUserID
		if s.UserBridge.IsDuplicateInsert(err) {
			return gopherbouncedb.InvalidUserID,
				gopherbouncedb.NewUserExists(fmt.Sprintf("unique constraint failed: %s", err.Error()))
		}
		return gopherbouncedb.InvalidUserID, err
	}
	user.ID = gopherbouncedb.UserID(userID)
	return gopherbouncedb.UserID(userID), nil
}

// PGSessionQueries implements gopherbouncedb.SessionSQL with support for Postgres.
type PGSessionQueries struct {
	InitS []string
	InsertSessionS, GetSessionS, DeleteSessionS, CleanUpSessionS, DeleteForUserSessionS string
	Replacer *gopherbouncedb.SQLTemplateReplacer
}

// NewPGSessionQueries returns new queries given the replacement mapping that is used to update
// the default replacer.
//
// That is it uses the default Postgres replacer, but updates the fields given in
// replaceMapping to overwrite existing values / insert new ones.
func NewPGSessionQueries(replaceMapping map[string]string) *PGSessionQueries {
	replacer := DefaultPostgreReplacer()
	if replaceMapping != nil {
		replacer.UpdateDict(replaceMapping)
	}
	res := &PGSessionQueries{}
	res.Replacer = replacer
	res.InitS = append(res.InitS, replacer.Apply(PGSessionsInit))
	res.InsertSessionS = replacer.Apply(PGInsertSession)
	res.GetSessionS = replacer.Apply(PGGetSession)
	res.DeleteSessionS = replacer.Apply(PGDeleteSession)
	res.CleanUpSessionS = replacer.Apply(PGCleanUpSession)
	res.DeleteForUserSessionS = replacer.Apply(PGDeleteSessionForUser)
	return res
}

func (q *PGSessionQueries) InitSessions() []string {
	return q.InitS
}

func (q *PGSessionQueries) GetSession() string {
	return q.GetSessionS
}

func (q *PGSessionQueries) InsertSession() string {
	return q.InsertSessionS
}

func (q *PGSessionQueries) DeleteSession() string {
	return q.DeleteSessionS
}

func (q *PGSessionQueries) CleanUpSession() string {
	return q.CleanUpSessionS
}

func (q *PGSessionQueries) DeleteForUserSession() string {
	return q.DeleteForUserSessionS
}

// PGSessionStorage is as session storage based on Postgres.
type PGSessionStorage struct {
	*gopherbouncedb.SQLSessionStorage
}

// NewPGSessionStorage creates a new Postgres session storage given the database connection
// and the replacement mapping used to create the queries with NewPGSessionQueries.
//
// If you want to configure any options please read the gopherbounce wiki.
func NewPGSessionStorage(db *sql.DB, replaceMapping map[string]string) *PGSessionStorage {
	queries := NewPGSessionQueries(replaceMapping)
	bridge := NewPGBridge()
	sqlStorage := gopherbouncedb.NewSQLSessionStorage(db, queries, bridge)
	return &PGSessionStorage{sqlStorage}
}

// PGStorage combines a user storage and a session storage (both based on Postgres)
// to implement gopherbouncedb.GoauthStorage.
type PGStorage struct {
	*PGUserStorage
	*PGSessionStorage
}

// NewPGStorage returns a new PGStorage.
func NewPGStorage(db *sql.DB, replaceMapping map[string]string) *PGStorage {
	return &PGStorage{
		NewPGUserStorage(db, replaceMapping),
		NewPGSessionStorage(db, replaceMapping),
	}
}