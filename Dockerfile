#
# build image
#
FROM golang:alpine as build

# install base build
RUN apk add build-base

WORKDIR /app

# cache deps
COPY go.mod go.sum ./
RUN go mod download -x

# build & test
COPY . .
RUN go build ./...
RUN go test -v ./...
RUN go build -o bin/crawler cmd/crawler/*.go

#
# final image
#
FROM golang:alpine

RUN apk add bash

WORKDIR /app

COPY --from=build /app/bin/crawler .

ENTRYPOINT ["/app/crawler"]