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

const (
	PGUsersInit = `CREATE TABLE IF NOT EXISTS $USERS_TABLE_NAME$ (
id BIGSERIAL PRIMARY KEY,
username VARCHAR(150) NOT NULL UNIQUE,
password VARCHAR(270) NOT NULL,
email VARCHAR(254) NOT NULL $EMAIL_UNIQUE$,
first_name VARCHAR(50) NOT NULL,
last_name VARCHAR(150) NOT NULL,
is_superuser BOOL NOT NULL,
is_staff BOOL NOT NULL,
is_active BOOL NOT NULL,
date_joined TIMESTAMP NOT NULL,
last_login TIMESTAMP NOT NULL
);`

	PGUsernameIndex = `CREATE INDEX IF NOT EXISTS idx_users_username 
ON $USERS_TABLE_NAME$(username);`

	PGEMailIndex = `CREATE INDEX IF NOT EXISTS idx_users_email
ON $USERS_TABLE_NAME$(email);`

	PGQueryUserID = `SELECT * FROM $USERS_TABLE_NAME$ WHERE id=$1;`

	PGQueryUsername = `SELECT * FROM $USERS_TABLE_NAME$ WHERE username=$1;`

	PQQueryEmail = `SELECT * FROM $USERS_TABLE_NAME$ WHERE email=$1;`

	PGInsertUser = `INSERT INTO $USERS_TABLE_NAME$(
username, password, email, first_name, last_name, is_superuser, is_staff,
is_active, date_joined, last_login)
VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
RETURNING id;`

	PGUpdateUser = `UPDATE $USERS_TABLE_NAME$
SET username=$1, password=$2, email=$3, first_name=$4, last_name=$5,
	is_superuser=$6, is_staff=$7, is_active=$8, date_joined=$9, last_login=$10
WHERE id=$11;`

	PGDeleteUser = `DELETE FROM $USERS_TABLE_NAME$ WHERE id=$1;`

	PGUpdateUserFields = `UPDATE $USERS_TABLE_NAME$
SET $UPDATE_CONTENT$
WHERE id=$ID_PARAM_NUM$;`
)
