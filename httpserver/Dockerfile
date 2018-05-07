ARG VERSION="latest"

FROM weberc2/jobmanager-base:${VERSION}

RUN go build -o jobmanager-http-server ./httpserver

FROM alpine:latest

RUN apk --no-cache add ca-certificates

COPY --from=0 /go/src/github.com/weberc2/jobmanager/jobmanager-http-server .

ENV PG_CONNECTION_STRING ""

CMD ["./jobmanager-http-server"]
