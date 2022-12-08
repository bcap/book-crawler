# base image for everything else
FROM alpine as base
RUN apk add go bash

# image with everything ready to be built
FROM base as pre-build
RUN apk add build-base
WORKDIR /app
# cache deps
COPY go.mod go.sum ./
RUN go mod download -x
# copy everything else
COPY . .

# build & test
FROM pre-build as build
RUN go build ./...
RUN go test -v ./...
RUN go build -o bin/crawler cmd/crawler/*.go

# final exported image
FROM base
WORKDIR /app
COPY --from=build /app/bin/crawler .
ENTRYPOINT ["/app/crawler"]