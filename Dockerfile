# syntax=docker/dockerfile:1
FROM golang:1.24-alpine AS builder

WORKDIR /app

# C compiler, LMDB headers
RUN apk add --no-cache gcc musl-dev linux-headers lmdb-dev

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN go build -tags 'osusergo netgo static_build' -ldflags '-extldflags "-static" -s -w' -o unisondb ./cmd/unisondb

FROM scratch

WORKDIR /root/
COPY --from=builder /app/unisondb .

ENTRYPOINT ["./unisondb"]
