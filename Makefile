build:
	go build -o ./bin/mirror ./cmd/mirror/main.go
	go build -o ./bin/handle ./cmd/handle/main.go

save:
	godep save ./...

test:
	go test ./{cluster,service}/...
	go test ./syncing/...

push-dockerhub:
	docker build -t adamringhede/influxdb-cluster -f Dockerfile . && \
	docker push adamringhede/influxdb-cluster
