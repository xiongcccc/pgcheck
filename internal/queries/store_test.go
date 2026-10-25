package queries

import "testing"

func TestQuoteIdentifier(t *testing.T) {
	for _, value := range []string{"public", "stats space", `stats"; SELECT 1; --`} {
		got := QuoteIdentifier(value)
		if got[0] != '"' || got[len(got)-1] != '"' {
			t.Fatalf("not quoted: %s", got)
		}
	}
	if got := QuoteIdentifier(`a"b`); got != `"a""b"` {
		t.Fatalf("bad escaping: %s", got)
	}
}
