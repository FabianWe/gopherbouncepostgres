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
	"os"
	"fmt"
	"github.com/FabianWe/gopherbouncedb"
	"log"
	"testing"
	"github.com/FabianWe/gopherbouncedb/testsuite"
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

type pgTestBinding struct{
	db *sql.DB
}

func newPGTestBinding() *pgTestBinding {
	return &pgTestBinding{nil}
}

func removeData(db *sql.DB) error {
	stmt := `DROP TABLE IF EXISTS auth_user;`
	_, err := db.Exec(stmt)
	return err
}

func (b *pgTestBinding) BeginInstance() gopherbouncedb.UserStorage {
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
	storage := NewPGUserStorage(db, nil)
	return storage
}

func (b *pgTestBinding) ClosteInstance(s gopherbouncedb.UserStorage) {
	if removeErr := removeData(b.db); removeErr != nil {
		log.Printf("can't delete table entries: %s\n", removeErr.Error())
	}
	if closeErr := b.db.Close(); closeErr != nil {
		panic(fmt.Sprintf("Can't close database: %s", closeErr.Error()))
	}
}

func TestInit(t *testing.T) {
	testsuite.TestInitSuite(newPGTestBinding(), t)
}

func TestInsert(t *testing.T) {
	testsuite.TestInsertSuite(newPGTestBinding(), true, t)
}

func TestLookup(t *testing.T) {
	testsuite.TestLookupSuite(newPGTestBinding(), true, t)
}

func TestUpdate(t *testing.T) {
	testsuite.TestUpdateUserSuite(newPGTestBinding(), true, t)
}

func TestDelete(t *testing.T) {
	testsuite.TestDeleteUserSuite(newPGTestBinding(), true, t)
}