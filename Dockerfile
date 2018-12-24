FROM golang:1.10

VOLUME ["/data/.influxdb-ha"]

WORKDIR /go/src/github.com/adamringhede/influxdb-ha
COPY . ./
RUN go build -o bin/handle cmd/handle/main.go
ENTRYPOINT bin/handle