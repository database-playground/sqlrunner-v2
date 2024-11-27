FROM docker.io/library/golang:1-alpine AS builder
RUN mkdir /src
WORKDIR /src

COPY go.mod go.sum* ./
RUN go mod download
COPY . /src/
ENV CGO_ENABLED=0

RUN go build -o ./bin/server

FROM alpine AS runtime
LABEL org.opencontainers.image.source="https://github.com/database-playground/sqlrunner-v2"

COPY --from=builder /src/bin/server /bin/server
CMD ["/bin/server"]
