FROM golang:1.22-bullseye
ENV GOPROXY=https://goproxy.cn,direct CGO_ENABLED=1
RUN apt-get update && apt-get install -y --no-install-recommends protobuf-compiler && rm -rf /var/lib/apt/lists/*
RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.33.0 && go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.3.0
WORKDIR /workspace/control
