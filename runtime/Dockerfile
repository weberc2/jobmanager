ARG VERSION="latest"

FROM weberc2/jobmanager-base:${VERSION}

RUN go build -o jobmanager-runtime ./runtime

FROM alpine:latest

RUN apk --no-cache add ca-certificates

COPY --from=0 /go/src/github.com/weberc2/jobmanager/jobmanager-runtime .

ENV PG_CONNECTION_STRING ""

CMD ["./jobmanager-runtime"]
