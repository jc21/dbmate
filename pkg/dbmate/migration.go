package dbmate

import (
	"errors"
	"fmt"
	"os"
	"regexp"
	"strings"
)

// MigrationOptions is an interface for accessing migration options
type MigrationOptions interface {
	Transaction() bool
}

type migrationOptions map[string]string

// Transaction returns whether or not this migration should run in a transaction
// Defaults to true.
func (m migrationOptions) Transaction() bool {
	return m["transaction"] != "false"
}

// Migration contains the migration contents and options
type Migration struct {
	Contents string
	Options  MigrationOptions
}

func (m *Migration) ContentsReplaced(drv Driver, doReplacement bool) string {
	if doReplacement {
		w := drv.GetWildcards()
		if len(w) > 0 {
			// Replace Content with Wildcards
			newContents := m.Contents
			for wildcard, replacement := range w {
				newContents = strings.ReplaceAll(newContents, fmt.Sprintf("{{%s}}", wildcard), replacement)
			}
			return newContents
		}
	}
	return m.Contents
}

// NewMigration constructs a Migration object
func NewMigration() Migration {
	return Migration{Contents: "", Options: make(migrationOptions)}
}

// parseMigration reads a migration file and returns (up, down, upSlave, downSlave Migration, error)
func parseMigration(path string) (Migration, Migration, Migration, Migration, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return NewMigration(), NewMigration(), NewMigration(), NewMigration(), err
	}
	return parseMigrationContents(string(data))
}

var (
	upRegExp              = regexp.MustCompile(`(?m)^--\s*migrate:up(\s*$|\s+\S+)`)
	downRegExp            = regexp.MustCompile(`(?m)^--\s*migrate:down(\s*$|\s+\S+)$`)
	upSlaveRegExp         = regexp.MustCompile(`(?m)^--\s*migrate:up:slave(\s*$|\s+\S+)`)
	downSlaveRegExp       = regexp.MustCompile(`(?m)^--\s*migrate:down:slave(\s*$|\s+\S+)$`)
	emptyLineRegExp       = regexp.MustCompile(`^\s*$`)
	commentLineRegExp     = regexp.MustCompile(`^\s*--`)
	whitespaceRegExp      = regexp.MustCompile(`\s+`)
	optionSeparatorRegExp = regexp.MustCompile(`:`)
	blockDirectiveRegExp  = regexp.MustCompile(`^--\s*migrate:(up|down)(:slave)?`)
)

// Error codes
var (
	ErrParseMissingUp      = errors.New("dbmate requires each migration to define an up block with '-- migrate:up'")
	ErrParseUnexpectedStmt = errors.New("dbmate does not support statements defined outside of the '-- migrate:up', '-- migrate:up:slave', '-- migrate:down' or '-- migrate:down:slave' blocks")
	ErrParseDuplicateBlock = errors.New("dbmate does not allow multiple blocks of the same type in the same file")
)

// parseMigrationContents parses the string contents of a migration.
// It will return four Migration objects, the first representing the "up"
// block and the second representing the "down" block and then the slaves.
// This function requires that at least an up block was defined and will
// otherwise return an error. This supports migration blocks being in
// any order. This will also ensure there are no duplicate blocks.
func parseMigrationContents(contents string) (Migration, Migration, Migration, Migration, error) {
	re := regexp.MustCompile(`(?m)^--\s*migrate\:(?:up|down)(?:\:slave)?( |\n)`)
	reFinds := re.FindAllIndex([]byte(contents), -1)

	blocks := make([][]int, 0)
	firstBlockCharacter := -1

	for idx, reFind := range reFinds {
		// Set the start and end for this block to the find, and the end
		thisBlock := []int{reFind[0], len(contents)}

		// Set this var once, for use later when checking for sql outside of a block
		if firstBlockCharacter < 0 {
			firstBlockCharacter = reFind[0]
		}

		// if this is not the first block, then we want to set the end of the previous
		// block to be the start of this one.
		if idx > 0 {
			blocks[idx-1][1] = reFind[0] - 1
		}
		blocks = append(blocks, thisBlock)
	}

	up := NewMigration()
	down := NewMigration()
	upSlave := NewMigration()
	downSlave := NewMigration()

	// Ensure there are blocks found
	if len(blocks) == 0 {
		return up, down, upSlave, downSlave, ErrParseMissingUp
	}

	for _, b := range blocks {
		s := substring(contents, b[0], b[1])

		if upRegExp.Find([]byte(s)) != nil {
			if up.Contents != "" {
				return up, down, upSlave, downSlave, ErrParseDuplicateBlock
			}
			up.Contents = s
			up.Options = parseMigrationOptions(s)
		}
		if downRegExp.Find([]byte(s)) != nil {
			if down.Contents != "" {
				return up, down, upSlave, downSlave, ErrParseDuplicateBlock
			}
			down.Contents = s
			down.Options = parseMigrationOptions(s)
		}
		if upSlaveRegExp.Find([]byte(s)) != nil {
			if upSlave.Contents != "" {
				return up, down, upSlave, downSlave, ErrParseDuplicateBlock
			}
			upSlave.Contents = s
			upSlave.Options = parseMigrationOptions(s)
		}
		if downSlaveRegExp.Find([]byte(s)) != nil {
			if downSlave.Contents != "" {
				return up, down, upSlave, downSlave, ErrParseDuplicateBlock
			}
			downSlave.Contents = s
			downSlave.Options = parseMigrationOptions(s)
		}
	}

	// Ensure there is at least an up block
	if up.Contents == "" {
		return up, down, upSlave, downSlave, ErrParseMissingUp
	}

	if statementsPrecedeMigrateBlocks(contents, firstBlockCharacter) {
		return up, down, upSlave, downSlave, ErrParseUnexpectedStmt
	}

	return up, down, upSlave, downSlave, nil
}

// parseMigrationOptions parses the migration options out of a block
// directive into an object that implements the MigrationOptions interface.
//
// For example:
//
//	fmt.Printf("%#v", parseMigrationOptions("-- migrate:up transaction:false"))
//	// migrationOptions{"transaction": "false"}
func parseMigrationOptions(contents string) MigrationOptions {
	options := make(migrationOptions)

	// strip away the -- migrate:[up|down] part
	contents = blockDirectiveRegExp.ReplaceAllString(contents, "")

	// remove leading and trailing whitespace
	contents = strings.TrimSpace(contents)

	// return empty options if nothing is left to parse
	if contents == "" {
		return options
	}

	// split the options string into pairs, e.g. "transaction:false foo:bar" -> []string{"transaction:false", "foo:bar"}
	stringPairs := whitespaceRegExp.Split(contents, -1)

	for _, stringPair := range stringPairs {
		// split stringified pair into key and value pairs, e.g. "transaction:false" -> []string{"transaction", "false"}
		pair := optionSeparatorRegExp.Split(stringPair, -1)

		// if the syntax is well-formed, then store the key and value pair in options
		if len(pair) == 2 {
			options[pair[0]] = pair[1]
		}
	}

	return options
}

// statementsPrecedeMigrateBlocks inspects the contents between the first character
// of a string and the index of the first block directive to see if there are any statements
// defined outside of the block directive. It'll return true if it finds any such statements.
//
// For example:
//
// This will return false:
//
// statementsPrecedeMigrateBlocks(`-- migrate:up
// create table users (id serial);
// `, 0, -1)
//
// This will return true:
//
// statementsPrecedeMigrateBlocks(`create type status_type as enum('active', 'inactive');
// -- migrate:up
// create table users (id serial, status status_type);
// `, 54, -1)
func statementsPrecedeMigrateBlocks(contents string, firstBlockStart int) bool {
	lines := strings.Split(contents[0:firstBlockStart], "\n")

	for _, line := range lines {
		if isEmptyLine(line) || isCommentLine(line) {
			continue
		}
		return true
	}

	return false
}

// isEmptyLine will return true if the line has no
// characters or if all the characters are whitespace characters
func isEmptyLine(s string) bool {
	return emptyLineRegExp.MatchString(s)
}

// isCommentLine will return true if the line is a SQL comment
func isCommentLine(s string) bool {
	return commentLineRegExp.MatchString(s)
}

func substring(s string, begin, end int) string {
	if begin == -1 || end == -1 {
		return ""
	}
	return s[begin:end]
}
