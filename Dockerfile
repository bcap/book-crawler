#
# build image
#
FROM golang:alpine as build

# install base build tools
RUN apk add build-base

WORKDIR /app

# cache deps
COPY go.mod go.sum ./
RUN go mod download -x

# build & test
COPY . .
RUN go build ./...
RUN go test -v ./...
RUN go build -o main main.go

#
# final image
#
FROM golang:alpine

WORKDIR /app

COPY --from=build /app/main .

ENTRYPOINT ./main