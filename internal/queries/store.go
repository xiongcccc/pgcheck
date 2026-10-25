package queries

import (
	"fmt"
	"io/fs"
	"strings"
)

type Store struct {
	fs fs.FS
}

func NewStore(source fs.FS) Store {
	return Store{fs: source}
}

func (s Store) Read(name string) (string, error) {
	path := "SQL/" + name
	data, err := fs.ReadFile(s.fs, path)
	if err != nil {
		return "", fmt.Errorf("read %s: %w", path, err)
	}
	return strings.TrimSpace(string(data)), nil
}

func QuoteLiteral(v string) string {
	return "'" + strings.ReplaceAll(v, "'", "''") + "'"
}

func QuoteIdentifier(v string) string {
	return `"` + strings.ReplaceAll(v, `"`, `""`) + `"`
}

func ReplaceLiteral(query, oldLiteral, value string) string {
	return strings.ReplaceAll(query, oldLiteral, QuoteLiteral(value))
}
