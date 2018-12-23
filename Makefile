build:
	docker build -f Dockerfile_relay -t influxdb-relay:latest .
	go build -o ./bin/routerd ./cmd/routerd/main.go
	go build -o ./bin/handle ./cmd/handle/main.go

save:
	godep save ./...

test:
	go test ./{cluster,service}/...
	go test ./syncing/...

push-dockerhub:
	docker build -t adamringhede/influxdb-cluster -f Dockerfile_handle . && \
	docker push adamringhede/influxdb-cluster
