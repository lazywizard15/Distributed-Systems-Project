FROM golang:1.26.4-alpine AS builder
WORKDIR /app

COPY go.mod .
RUN touch go.sum
COPY go.sum* .

RUN go mod download

COPY . .
RUN go build -o node ./cmd/node

FROM alpine:3.18
WORKDIR /app
COPY --from=builder /app/node .
EXPOSE 8080
CMD ["./node"]