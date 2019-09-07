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
	"github.com/FabianWe/gopherbouncedb"
	"time"
	"fmt"
	"reflect"
	"github.com/lib/pq"
	"database/sql"
	"strings"
	"strconv"
)

const (
	POSTGRE_KEY_EXITS = "23505"
)

type PostgreQueries struct {
	InitS                                                                            []string
	GetUserS, GetUserByNameS, GetUserByEmailS, InsertUserS, UpdateUserS, DeleteUserS string
	Replacer *gopherbouncedb.SQLTemplateReplacer
}

func DefaultPostgreReplacer() *gopherbouncedb.SQLTemplateReplacer {
	return gopherbouncedb.DefaultSQLReplacer()
}

func NewMPostgreQueries(replaceMapping map[string]string) *PostgreQueries {
	replacer := DefaultPostgreReplacer()
	if replaceMapping != nil {
		replacer.UpdateDict(replaceMapping)
	}
	res := &PostgreQueries{}
	res.Replacer = replacer
	// first all init strings
	res.InitS = append(res.InitS, replacer.Apply(POSTGRE_USERS_INIT))
	res.GetUserS = replacer.Apply(POSTGRE_QUERY_USERID)
	res.GetUserByNameS = replacer.Apply(POSTGRE_QUERY_USERNAME)
	res.GetUserByEmailS = replacer.Apply(POSTGRE_QUERY_USERMAIL)
	res.InsertUserS = replacer.Apply(POSTGRE_INSERT_USER)
	res.UpdateUserS = replacer.Apply(POSTGRE_UPDATE_USER)
	res.DeleteUserS = replacer.Apply(POSTGRE_DELETE_USER)
	return res
}

func (q *PostgreQueries) Init() []string {
	return q.InitS
}

func (q *PostgreQueries) GetUser() string {
	return q.GetUserS
}

func (q *PostgreQueries) GetUserByName() string {
	return q.GetUserByNameS
}

func (q *PostgreQueries) GetUserByEmail() string {
	return q.GetUserByEmailS
}

func (q *PostgreQueries) InsertUser() string {
	return q.InsertUserS
}

func (q *PostgreQueries) UpdateUser(fields []string) string {
	return q.UpdateUserS
}

func (q *PostgreQueries) DeleteUser() string {
	return q.DeleteUserS
}

type MyPostgreBridge struct{}

func NewMyPostgreBridge() MyPostgreBridge {
	return MyPostgreBridge{}
}

func (b MyPostgreBridge) TimeScanType() interface{} {
	var res time.Time
	return &res
}

func (b MyPostgreBridge) ConvertTimeScanType(val interface{}) (time.Time, error) {
	switch v := val.(type) {
	case *time.Time:
		return *v, nil
	case time.Time:
		return v, nil
	default:
		var zeroT time.Time
		return zeroT, fmt.Errorf("MyPostgreBridge.ConvertTimeScanType: Expected value of *time.Time, got %v",
			reflect.TypeOf(val))
	}
}

func (b MyPostgreBridge) ConvertTime(t time.Time) interface{} {
	return t
}

func (b MyPostgreBridge) ConvertExistsErr(err error) error {
	if postgreErr, ok := err.(*pq.Error); ok && postgreErr.Code == POSTGRE_KEY_EXITS {
		return gopherbouncedb.NewUserExists(fmt.Sprintf("unique constrained failed: %s", postgreErr.Error()))
	}
	return err
}

func (b MyPostgreBridge) ConvertAmbiguousErr(err error) error {
	if postgreErr, ok := err.(*pq.Error); ok && postgreErr.Code == POSTGRE_KEY_EXITS {
		return gopherbouncedb.NewAmbiguousCredentials(fmt.Sprintf("unique constrained failed: %s", postgreErr.Error()))
	}
	return err
}

var (
	DefaultPostgreRowNames = gopherbouncedb.DefaultUserRowNames
)

type PostgreStorage struct {
	*gopherbouncedb.SQLUserStorage
	UpdateFieldsS string
}

func NewPostgreStorage(db *sql.DB, replaceMapping map[string]string) *PostgreStorage {
	queries := NewMPostgreQueries(replaceMapping)
	bridge := NewMyPostgreBridge()
	sqlStorage := gopherbouncedb.NewSQLUserStorage(db, queries, bridge)
	// TODO
	res := PostgreStorage{sqlStorage, queries.Replacer.Apply(POSTGRE_UPDATE_USER_FIELDS)}
	return &res
}

// overwrites because of returning, must use query row
func (s *PostgreStorage) InsertUser(user *gopherbouncedb.UserModel) (gopherbouncedb.UserID, error) {
	now := time.Now().UTC()
	var zeroTime time.Time
	// use the bridge conversion for time
	// we do this because the bridge could be changed and doing this direct insert could go wrong
	dateJoined := s.Bridge.ConvertTime(now)
	lastLogin := s.Bridge.ConvertTime(zeroTime)
	user.DateJoined = now
	user.LastLogin = zeroTime
	var userID gopherbouncedb.UserID
	err := s.DB.QueryRow(s.Queries.InsertUser(),
		user.Username, user.Password, user.EMail, user.FirstName,
		user.LastName, user.IsSuperUser, user.IsStaff,
		user.IsActive, dateJoined, lastLogin).Scan(&userID)
	if err != nil {
		user.ID = gopherbouncedb.InvalidUserID
		return gopherbouncedb.InvalidUserID, s.Bridge.ConvertExistsErr(err)
	}
	return gopherbouncedb.UserID(userID), nil
}

func (s *PostgreStorage) UpdateUser(id gopherbouncedb.UserID, newCredentials *gopherbouncedb.UserModel, fields []string) error {
	// if internal method not supplied or no fields given: use simple update from sql
	if s.UpdateFieldsS == "" || len(fields) == 0 {
		return s.SQLUserStorage.UpdateUser(id, newCredentials, fields)
	}
	// now perform a more sophisticated update
	updates := make([]string, len(fields))
	args := make([]interface{}, len(fields), len(fields) + 1)
	for i, fieldName := range fields {
		if colName, has := DefaultPostgreRowNames[fieldName]; has {
			updates[i] = colName + "=$" + strconv.Itoa(i+1)
		} else {
			return fmt.Errorf("invalid field name \"%s\": Must be a valid field name of the user model", fieldName)
		}
		if arg, argErr := newCredentials.GetFieldByName(fieldName); argErr == nil {
			fieldName = strings.ToLower(fieldName)
			if fieldName == "datejoined" || fieldName == "lastlogin" {
				if t, isTime := arg.(time.Time); isTime {
					arg = s.Bridge.ConvertTime(t)
				} else {
					return fmt.Errorf("DateJoined / LastLogin must be time.Time, got type %v", reflect.TypeOf(arg))
				}
			}
			args[i] = arg
		} else {
			return argErr
		}
	}
	// append id to args
	args = append(args, id)
	// prepare update string
	updateStr := strings.Join(updates, ",")
	// replace updateStr and id param num in UpdateFieldS
	// we don't create a replacer, too much overhead for just two entries
	stmt := strings.Replace(s.UpdateFieldsS, "$UPDATE_CONTENT$", updateStr, 1)
	stmt = strings.Replace(stmt, "$ID_PARAM_NUM$", "$"+strconv.Itoa(len(fields)+1), 1)
	// execute statement
	_, err := s.DB.Exec(stmt, args...)
	if err != nil {
		return s.Bridge.ConvertAmbiguousErr(err)
	}
	return nil
}