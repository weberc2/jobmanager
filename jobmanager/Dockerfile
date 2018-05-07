FROM golang:alpine

WORKDIR /go/src/github.com/weberc2/jobmanager

RUN adduser -D user

ADD . .

RUN chown -R user:user .

USER user

RUN CGO_ENABLED=0 go install -v ./jobmanager
