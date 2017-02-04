build:
	docker build -f dockerfile_relay -t influxdb-relay:latest .
	go build -o ./bin/routerd ./cmd/routerd/main.go