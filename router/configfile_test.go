package router

import "testing"

func TestParseConfigFile(t *testing.T) {
	ParseConfigFile("../ha-router.toml")
}