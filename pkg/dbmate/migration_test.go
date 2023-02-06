package dbmate

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/amacneil/dbmate/pkg/dbutil"
)

func TestParseMigrationContents(t *testing.T) {
	type want struct {
		up                   string
		down                 string
		upSlave              string
		downSlave            string
		upTransaction        bool
		downTransaction      bool
		upSlaveTransaction   bool
		downSlaveTransaction bool
		err                  error
	}

	tests := []struct {
		name     string
		contents string
		want     want
	}{
		{
			name: "Typical use case",
			contents: `-- migrate:up
create table users (id serial, name text);
-- migrate:down
drop table users;`,
			want: want{
				up:              "-- migrate:up\ncreate table users (id serial, name text);",
				down:            "-- migrate:down\ndrop table users;",
				upTransaction:   true,
				downTransaction: true,
			},
		},
		{
			name: "does not require space between the '--' and 'migrate'",
			contents: `
--migrate:up
create table users (id serial, name text);

--migrate:down
drop table users;
`,
			want: want{
				up:              "--migrate:up\ncreate table users (id serial, name text);\n",
				down:            "--migrate:down\ndrop table users;\n",
				upTransaction:   true,
				downTransaction: true,
			},
		},
		{
			name: "down to be defined before up",
			contents: `-- migrate:down
drop table users;
-- migrate:up
create table users (id serial, name text);
`,
			want: want{
				up:              "-- migrate:up\ncreate table users (id serial, name text);\n",
				down:            "-- migrate:down\ndrop table users;",
				upTransaction:   true,
				downTransaction: true,
			},
		},
		{
			name: "turning transactions off for a given migration block",
			contents: `-- migrate:up transaction:false
ALTER TYPE colors ADD VALUE 'orange' AFTER 'red';
`,
			want: want{
				up:            "-- migrate:up transaction:false\nALTER TYPE colors ADD VALUE 'orange' AFTER 'red';\n",
				upTransaction: false,
			},
		},
		{
			name: "It does *not* support omitting the up block",
			contents: `-- migrate:down
drop table users;
`,
			want: want{
				err: errors.New("dbmate requires each migration to define an up block with '-- migrate:up'"),
			},
		},
		{
			name: "allows leading comments and whitespace preceding the migrate blocks",
			contents: `
-- This migration creates the users table.
-- It'll drop it in the event of a rollback.

-- migrate:up
create table users (id serial, name text);

-- migrate:down
drop table users;
`,
			want: want{
				up:              "-- migrate:up\ncreate table users (id serial, name text);\n",
				upTransaction:   true,
				down:            "-- migrate:down\ndrop table users;\n",
				downTransaction: true,
			},
		},
		{
			name: "does *not* allow arbitrary statements preceding the migrate blocks",
			contents: `
-- create status_type
CREATE TYPE status_type AS ENUM ('active', 'inactive');

-- migrate:up
ALTER TABLE users
ADD COLUMN status status_type DEFAULT 'active';

-- migrate:down
ALTER TABLE users
DROP COLUMN status;
`,
			want: want{
				err: ErrParseUnexpectedStmt,
			},
		},
		{
			name: "requires an at least an up block",
			contents: `
ALTER TABLE users
ADD COLUMN status status_type DEFAULT 'active';
`,
			want: want{
				err: ErrParseMissingUp,
			},
		},
		{
			name: "does not allow duplicate blocks",
			contents: `
-- migrate:up
ADD COLUMN status status_type DEFAULT 'active';

-- migrate:up transaction:false
ADD COLUMN status status_type DEFAULT 'active';
`,
			want: want{
				err: ErrParseDuplicateBlock,
			},
		},
		{
			name: "Slaves",
			contents: `-- migrate:up
create table users (id serial, name text);
-- migrate:up:slave
create table users (id serial, name text);
-- migrate:down
drop table users;
-- migrate:down:slave
drop table users;`,
			want: want{
				up:                   "-- migrate:up\ncreate table users (id serial, name text);",
				upSlave:              "-- migrate:up:slave\ncreate table users (id serial, name text);",
				down:                 "-- migrate:down\ndrop table users;",
				downSlave:            "-- migrate:down:slave\ndrop table users;",
				upTransaction:        true,
				upSlaveTransaction:   true,
				downTransaction:      true,
				downSlaveTransaction: true,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			up, down, upSlave, downSlave, err := parseMigrationContents(tt.contents)
			assert.Equal(t, tt.want.err, err)
			if tt.want.err == nil {
				assert.Equal(t, tt.want.up, up.Contents)
				assert.Equal(t, tt.want.down, down.Contents)
				assert.Equal(t, tt.want.upSlave, upSlave.Contents)
				assert.Equal(t, tt.want.downSlave, downSlave.Contents)
				if tt.want.up != "" {
					assert.Equal(t, tt.want.upTransaction, up.Options.Transaction())
				}
				if tt.want.down != "" {
					assert.Equal(t, tt.want.downTransaction, down.Options.Transaction())
				}
				if tt.want.upSlave != "" {
					assert.Equal(t, tt.want.upSlaveTransaction, upSlave.Options.Transaction())
				}
				if tt.want.downSlave != "" {
					assert.Equal(t, tt.want.downSlaveTransaction, downSlave.Options.Transaction())
				}
			}
		})
	}
}

func TestParseMigrationContentsReplaced(t *testing.T) {
	type want struct {
		up   string
		down string
	}

	tests := []struct {
		name      string
		wildcards bool
		dbURL     string
		contents  string
		want      want
	}{
		{
			name:      "Typical use case, wildcards disabled",
			wildcards: false,
			dbURL:     "postgres://postgres:postgres@postgres/dbmate_test?sslmode=disable",
			contents: `-- migrate:up
create table users (id serial, name text);
-- migrate:down
drop table users;`,
			want: want{
				up:   "-- migrate:up\ncreate table users (id serial, name text);",
				down: "-- migrate:down\ndrop table users;",
			},
		},
		{
			name:      "Typical use case, wildcards enabled",
			wildcards: true,
			dbURL:     "postgres://replicant@postgres-slave/dbmate_test?sslmode=disable&search_path=microservices",
			contents: `-- migrate:up
create table users (id serial, name text);
-- migrate:down
drop table users;`,
			want: want{
				up:   "-- migrate:up\ncreate table users (id serial, name text);",
				down: "-- migrate:down\ndrop table users;",
			},
		},
		{
			name:      "Typical use case, wildcards disabled with some set",
			wildcards: false,
			dbURL:     "postgres://postgres:postgres@postgres/dbmate_test?sslmode=disable",
			contents: `-- migrate:up
create table users (id serial, name text);
grant select on all tables in schema {{DB_SCHEMA}} to {{DB_USER}};
-- migrate:down
drop table users;`,
			want: want{
				up:   "-- migrate:up\ncreate table users (id serial, name text);\ngrant select on all tables in schema {{DB_SCHEMA}} to {{DB_USER}};",
				down: "-- migrate:down\ndrop table users;",
			},
		},
		{
			name:      "Typical use case, wildcards enabled with some set",
			wildcards: true,
			dbURL:     "postgres://replicant@postgres-slave/dbmate_test?sslmode=disable&search_path=microservices",
			contents: `-- migrate:up
create table users (id serial, name text);
grant select on all tables in schema {{DB_SCHEMA}} to '{{DB_USER}}';
-- migrate:down
drop table users;`,
			want: want{
				up:   "-- migrate:up\ncreate table users (id serial, name text);\ngrant select on all tables in schema microservices to 'replicant';",
				down: "-- migrate:down\ndrop table users;",
			},
		},
		{
			name:      "Test all wildcards",
			wildcards: true,
			dbURL:     "postgres://replicant:abc123@postgres-slave/dbmate_slave?sslmode=disable",
			contents: `-- migrate:up
-- DB_NAME: {{DB_NAME}}
-- DB_USER: {{DB_USER}}
-- DB_PASS: {{DB_PASS}}
-- DB_SCHEMA: {{DB_SCHEMA}}
`,
			want: want{
				up: `-- migrate:up
-- DB_NAME: dbmate_slave
-- DB_USER: replicant
-- DB_PASS: abc123
-- DB_SCHEMA: public
`,
				down: "",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			u := dbutil.MustParseURL(tt.dbURL)
			db := New(u, nil)
			db.AutoDumpSchema = false
			drv, _ := db.GetDriver()

			up, down, _, _, err := parseMigrationContents(tt.contents)
			assert.Equal(t, nil, err)
			assert.Equal(t, tt.want.up, up.ContentsReplaced(drv, tt.wildcards))
			assert.Equal(t, tt.want.down, down.ContentsReplaced(drv, tt.wildcards))
		})
	}
}
