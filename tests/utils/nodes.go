package utils

import (
	"os/exec"
)

type NodeHost []string

var (
	Nodes = []NodeHost{
		{"influxdb-handle", "influxdb-1"},
		{"influxdb-handle2", "influxdb-2"},
		{"influxdb-handle3", "influxdb-3"},
	}
)

func StartNode(node NodeHost) {
	exec.Command("docker-compose", "start", node[1]).Run()
	exec.Command("docker-compose", "start", node[0]).Run()
}

func StopNode(node NodeHost) {
	exec.Command("docker-compose", "stop", node[0]).Run()
	exec.Command("docker-compose", "stop", node[1]).Run()
}