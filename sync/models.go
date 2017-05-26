package sync

import "github.com/influxdata/influxdb/models"

// Message represents a user-facing message to be included with the result.
type message struct {
	Level string `json:"level"`
	Text  string `json:"text"`
}

type result struct {
	StatementID int           `json:"statement_id"`
	Series      []*models.Row `json:"series,omitempty"`
	Messages    []*message    `json:"messages,omitempty"`
	Partial     bool          `json:"partial,omitempty"`
	Err         string        `json:"error,omitempty"`
}

type response struct {
	Results []result `json:"results"`
}