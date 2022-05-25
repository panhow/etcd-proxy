FROM golang:1.14
LABEL description="etcd-proxy"
WORKDIR /build/etcd-proxy
ADD . .
RUN go mod download && \
    go build -o bin/etcd-proxy ./main.go
EXPOSE 80
CMD ["./bin/etcd-proxy"]
