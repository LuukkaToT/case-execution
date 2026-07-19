# 构建阶段只保留编译所需内容。
FROM golang:1.25-alpine AS builder

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/execution-engine ./cmd/server

# 运行阶段使用最小镜像；生产环境应通过只读挂载或环境变量注入配置。
FROM alpine:3.22

RUN apk add --no-cache ca-certificates tzdata \
    && addgroup -S app \
    && adduser -S -G app app
WORKDIR /app
COPY --from=builder /out/execution-engine ./execution-engine
COPY configs/config.yaml ./configs/config.yaml
USER app
EXPOSE 9090
ENTRYPOINT ["./execution-engine"]
