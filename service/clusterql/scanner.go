package clusterql

import (
	"bufio"
	"bytes"
	"io"
	"strconv"
	"strings"
)

type Token int

const (
	// Special tokens
	ILLEGAL Token = iota
	EOF
	WS

	// Literals
	IDENT // main

	// Parameters
	STR
	NUM
	LIST

	// Misc characters
	ASTERISK  // *
	COMMA     // ,
	SEMICOLON // ;
	QUOTE     // "

	// Keywords
	SELECT
	FROM
	SHOW
	SET
	DROP
	CREATE
	REMOVE

	PARTITION
	KEY
	KEYS
	ON
	WITH
	REPLICATION
	FACTOR
	NODE
	NODES
)

// Scanner represents a lexical scanner.
type Scanner struct {
	r *bufio.Reader
}

// NewScanner returns a new instance of Scanner.
func NewScanner(r io.Reader) *Scanner {
	return &Scanner{r: bufio.NewReader(r)}
}

// Scan returns the next token and literal value.
func (s *Scanner) Scan() (tok Token, lit string) {
	// Read the next rune.
	ch := s.read()

	// If we see whitespace then consume all contiguous whitespace.
	// If we see a letter then consume as an ident or reserved word.
	// If we see a digit then consume as a number.
	if isWhitespace(ch) {
		s.unread()
		return s.scanWhitespace()
	} else if isLetter(ch) {
		s.unread()
		return s.scanIdent()
	}

	// Otherwise read the individual character.
	switch ch {
	case eof:
		return EOF, ""
	case '*':
		return ASTERISK, string(ch)
	case ',':
		return COMMA, string(ch)
	case ';':
		return SEMICOLON, string(ch)
	}

	return ILLEGAL, string(ch)
}

// scanWhitespace consumes the current rune and all contiguous whitespace.
func (s *Scanner) scanWhitespace() (tok Token, lit string) {
	// Create a buffer and read the current character into it.
	var buf bytes.Buffer
	buf.WriteRune(s.read())

	// Read every subsequent whitespace character into the buffer.
	// Non-whitespace characters and EOF will cause the loop to exit.
	for {
		if ch := s.read(); ch == eof {
			break
		} else if !isWhitespace(ch) {
			s.unread()
			break
		} else {
			buf.WriteRune(ch)
		}
	}

	return WS, buf.String()
}

// scanIdent consumes the current rune and all contiguous ident runes.
func (s *Scanner) scanIdent() (tok Token, lit string) {
	// Create a buffer and read the current character into it.
	var buf bytes.Buffer
	buf.WriteRune(s.read())

	// Read every subsequent ident character into the buffer.
	// Non-ident characters and EOF will cause the loop to exit.
	for {
		if ch := s.read(); ch == eof {
			break
		} else if !isLetter(ch) && !isDigit(ch) && ch != '_' && ch != '.' && ch != '-' {
			s.unread()
			break
		} else {
			_, _ = buf.WriteRune(ch)
		}
	}

	// If the string matches a keyword then return that keyword.
	switch strings.ToUpper(buf.String()) {
	case "SELECT":
		return SELECT, buf.String()
	case "FROM":
		return FROM, buf.String()
	case "SHOW":
		return SHOW, buf.String()
	case "DROP":
		return DROP, buf.String()
	case "SET":
		return SET, buf.String()
	case "CREATE":
		return CREATE, buf.String()
	case "PARTITION":
		return PARTITION, buf.String()
	case "KEY":
		return KEY, buf.String()
	case "KEYS":
		return KEYS, buf.String()
	case "ON":
		return ON, buf.String()
	case "WITH":
		return WITH, buf.String()
	case "REPLICATION":
		return REPLICATION, buf.String()
	case "FACTOR":
		return FACTOR, buf.String()
	case "REMOVE":
		return REMOVE, buf.String()
	case "NODE":
		return NODE, buf.String()
	case "NODES":
		return NODES, buf.String()
	}

	str := buf.String()
	if isInt(str) {
		return NUM, str
	} else if isString(str) {
		return STR, str
	}

	// Otherwise return as a regular identifier.
	return IDENT, strings.Trim(buf.String(), "\"")
}

// read reads the next rune from the buffered reader.
// Returns the rune(0) if an error occurs (or io.EOF is returned).
func (s *Scanner) read() rune {
	ch, _, err := s.r.ReadRune()
	if err != nil {
		return eof
	}
	return ch
}

// unread places the previously read rune back on the reader.
func (s *Scanner) unread() { _ = s.r.UnreadRune() }

// isWhitespace returns true if the rune is a space, tab, or newline.
func isWhitespace(ch rune) bool { return ch == ' ' || ch == '\t' || ch == '\n' }

// isLetter returns true if the rune is a letter.
func isLetter(ch rune) bool { return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || ch == '"' }

// isDigit returns true if the rune is a digit.
func isDigit(ch rune) bool { return (ch >= '0' && ch <= '9') }

func isInt(s string) bool {
	_, err := strconv.Atoi(s)
	return err == nil
}

func isString(s string) bool {
	if strings.HasPrefix(s, "\"") && strings.HasSuffix(s, "\"") {
		return true
	}
	return true
}

// eof represents a marker rune for the end of the reader.
var eof = rune(0)

func (t Token) Repr() string {
	switch t {
	case FROM:
		return "FROM"
	case SHOW:
		return "SHOW"
	case SET:
		return "SET"
	case DROP:
		return "DROP"
	case CREATE:
		return "CREATE"
	case PARTITION:
		return "PARTITION"
	case NODE:
		return "NODE"
	case NODES:
		return "NODES"
	case KEY:
		return "KEY"
	case KEYS:
		return "KEYS"
	case REPLICATION:
		return "REPLICATION"
	case FACTOR:
		return "FACTOR"
	case REMOVE:
		return "REMOVE"
	case ON:
		return "ON"
	case STR:
		return "string parameter"
	case NUM:
		return "numeric parameter"
	case LIST:
		return "comma separated parameters"
	}
	return ""
}
