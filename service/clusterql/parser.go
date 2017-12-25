package clusterql

import (
	//"fmt"
	"fmt"
	"io"
	"strings"
)

// Parser represents a parser.
type Parser struct {
	s   *Scanner
	buf struct {
		tok Token  // last read token
		lit string // last read literal
		n   int    // buffer size (max=1)
	}
	lang *Language
}

// NewParser returns a new instance of Parser.
func NewParser(r io.Reader, lang *Language) *Parser {
	return &Parser{s: NewScanner(r), lang: lang}
}

func tokensToString(m map[Token]*Tree) string {
	var res []string
	for t := range m {
		res = append(res, t.Repr())
	}
	return strings.Join(res, ", ")
}

// Parse parses a SQL statement
func (p *Parser) Parse() (Statement, error) {
	tok, lit := p.scanIgnoreWhitespace()
	if _, ok := p.lang.trees[tok]; !ok {
		return nil, fmt.Errorf("found %q, expected %s", lit, tokensToString(p.lang.trees))
	}
	tree := p.lang.trees[tok]
	params := Params{}
	for {
		tok, lit = p.scanIgnoreWhitespace()
		if tok == EOF {
			if tree.Handler != nil {
				return tree.Handler(params), nil
			} else if len(tree.Children) == 0 {
				return nil, fmt.Errorf("internal error: a language spec leaf must have a handler")
			} else {
				return nil, fmt.Errorf("unexpected end of statement, expecting %s", tokensToString(tree.Children))
			}
			break
		}
		if _, ok := tree.Children[tok]; !ok {
			return nil, fmt.Errorf("found %q, expected %s", lit, tokensToString(tree.Children))
		}
		if tok == STR || tok == NUM {
			params = append(params, lit)
		}
		tree = tree.Children[tok]
	}
	return nil, nil
}

// scan returns the next token from the underlying scanner.
// If a token has been unscanned then read that instead.
func (p *Parser) scan() (tok Token, lit string) {
	// If we have a token on the buffer, then return it.
	if p.buf.n != 0 {
		p.buf.n = 0
		return p.buf.tok, p.buf.lit
	}

	// Otherwise read the next token from the scanner.
	tok, lit = p.s.Scan()

	// Save it to the buffer in case we unscan later.
	p.buf.tok, p.buf.lit = tok, lit

	return
}

// scanIgnoreWhitespace scans the next non-whitespace token.
func (p *Parser) scanIgnoreWhitespace() (tok Token, lit string) {
	tok, lit = p.scan()
	if tok == WS {
		tok, lit = p.scan()
	}
	return
}

// unscan pushes the previously read token back onto the buffer.
func (p *Parser) unscan() { p.buf.n = 1 }
