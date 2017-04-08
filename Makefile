build:
	docker build -f Dockerfile_relay -t influxdb-relay:latest .
	go build -o ./bin/routerd ./cmd/routerd/main.go
	go build -o ./bin/handle ./cmd/handle/main.go

save:
	godep save ./...