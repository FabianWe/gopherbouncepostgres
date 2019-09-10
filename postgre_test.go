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
	"github.com/FabianWe/gopherbouncedb/testsuite"
	"log"
	"os"
	"testing"
)

func setupPostgreConfigString() string {
	pgHost := os.Getenv("PGHOST")
	if pgHost == "" {
		pgHost = "localhost"
	}
	pgPort := os.Getenv("PGPORT")
	if pgPort == "" {
		pgPort = "5432"
	}
	pgUser := os.Getenv("PGUSER")
	if pgUser == "" {
		pgUser = "postgres"
	}
	pgPW := os.Getenv("PGPW")
	if pgPW == "" {
		pgPW = "password"
	}
	pgDatabase := os.Getenv("PGDATABASE")
	if pgDatabase == "" {
		pgDatabase = "postgres"
	}
	config := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		pgHost, pgPort, pgUser, pgPW, pgDatabase)
	return config
}

type pgUserTestBinding struct {
	db *sql.DB
}

func newPGUserTestBinding() *pgUserTestBinding {
	return &pgUserTestBinding{nil}
}

func removeData(db *sql.DB) error {
	stmt := `DROP TABLE IF EXISTS auth_session;`
	_, err := db.Exec(stmt)
	if err != nil {
		return err
	}
	stmt = `DROP TABLE IF EXISTS auth_user;`
	_, err = db.Exec(stmt)
	return err
}

func (b *pgUserTestBinding) BeginInstance() gopherbouncedb.UserStorage {
	// create db
	db, dbErr := sql.Open("postgres", setupPostgreConfigString())
	if dbErr != nil {
		panic(fmt.Sprintf("Can't create database: %s", dbErr.Error()))
	}
	b.db = db
	// clear tables
	if removeErr := removeData(b.db); removeErr != nil {
		log.Printf("can't delete table entries: %s\n", removeErr.Error())
	}
	return NewPGUserStorage(db, nil)
}

func (b *pgUserTestBinding) CloseInstance(s gopherbouncedb.UserStorage) {
	if removeErr := removeData(b.db); removeErr != nil {
		log.Printf("can't delete table entries: %s\n", removeErr.Error())
	}
	if closeErr := b.db.Close(); closeErr != nil {
		panic(fmt.Sprintf("Can't close database: %s", closeErr.Error()))
	}
}

func TestInit(t *testing.T) {
	testsuite.TestInitSuite(newPGUserTestBinding(), t)
}

func TestInsert(t *testing.T) {
	testsuite.TestInsertSuite(newPGUserTestBinding(), true, t)
}

func TestLookup(t *testing.T) {
	testsuite.TestLookupSuite(newPGUserTestBinding(), true, t)
}

func TestUpdate(t *testing.T) {
	testsuite.TestUpdateUserSuite(newPGUserTestBinding(), true, t)
}

func TestDelete(t *testing.T) {
	testsuite.TestDeleteUserSuite(newPGUserTestBinding(), true, t)
}

type pgSessionTestBinding struct {
	db *sql.DB
}

func newPSessionTestBinding() *pgSessionTestBinding {
	return &pgSessionTestBinding{nil}
}

func (b *pgSessionTestBinding) BeginInstance() gopherbouncedb.SessionStorage {
	// create db
	db, dbErr := sql.Open("postgres", setupPostgreConfigString())
	if dbErr != nil {
		panic(fmt.Sprintf("can't create database: %s", dbErr.Error()))
	}
	b.db = db
	// clear tables
	if removeErr := removeData(b.db); removeErr != nil {
		log.Printf("can't delete table entries: %s\n", removeErr.Error())
	}
	// pg requires the referenced table to exist
	s := NewPGUserStorage(db, nil)
	if createUserErr := s.InitUsers(); createUserErr != nil {
		log.Printf("failed to create user table: %s\n", createUserErr.Error())
	}
	// pg also requires the users to exist...
	// so we create some dummy users
	u1 := &gopherbouncedb.UserModel{}
	u1.Username = "u1"
	u1.EMail = "u1@foo.de"
	u2 := &gopherbouncedb.UserModel{}
	u2.Username = "u2"
	u2.EMail = "u2@bar.de"
	u3 := &gopherbouncedb.UserModel{}
	u3.Username = "u3"
	u3.EMail = "u3@bla.de"
	users := []*gopherbouncedb.UserModel{u1, u2, u3}
	for _, u := range users {
		if _, insertErr := s.InsertUser(u); insertErr != nil {
			log.Printf("Failed to insert dummy users: %s\n", insertErr.Error())
		}
	}
	return NewPGSessionStorage(db, nil)
}

func (b *pgSessionTestBinding) CloseInstance(s gopherbouncedb.SessionStorage) {
	if removeErr := removeData(b.db); removeErr != nil {
		log.Printf("can't delete table entries: %s\n", removeErr.Error())
	}
	if closeErr := b.db.Close(); closeErr != nil {
		panic(fmt.Sprintf("Can't close database: %s", closeErr.Error()))
	}
}

func TestSessionInit(t *testing.T) {
	testsuite.TestInitSessionSuite(newPSessionTestBinding(), t)
}

func TestSessionInsert(t *testing.T) {
	testsuite.TestSessionInsert(newPSessionTestBinding(), t)
}

func TestSessionGet(t *testing.T) {
	testsuite.TestSessionGet(newPSessionTestBinding(), t)
}

func TestSessionDelete(t *testing.T) {
	testsuite.TestSessionDelete(newPSessionTestBinding(), t)
}

func TestSessionCleanUp(t *testing.T) {
	testsuite.TestSessionCleanUp(newPSessionTestBinding(), t)
}

func TestSessionDeleteForUser(t *testing.T) {
	testsuite.TestSessionDeleteForUser(newPSessionTestBinding(), t)
}

