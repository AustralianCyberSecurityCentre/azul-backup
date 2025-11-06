# Start from a Debian image with the latest version of Go installed
# and a workspace (GOPATH) configured at /go.
ARG REGISTRY="docker.io/library"
ARG BASE_IMAGE=golang
ARG BASE_TAG=1.25-trixie

FROM $REGISTRY/$BASE_IMAGE:$BASE_TAG AS builder
ENV DEBIAN_FRONTEND=noninteractive
ENV GOOS=linux GOARCH=amd64 CGO_ENABLED=0 GO111MODULE=on GOPATH=/src/tmp/go
ENV GOPRIVATE=github.com/AustralianCyberSecurityCentre/*
ARG XDG_CONFIG_HOME

RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive \
    apt-get upgrade -y

RUN git config --global url."git@github.com:AustralianCyberSecurityCentre/".insteadOf "https://github.com/AustralianCyberSecurityCentre/"

# copy somewhere outside GOPATH/src AS using go modules
COPY . /src

# full static builds with no ld deps, so we can copy it to scratch
RUN --mount=type=ssh,id=id cd /src && \
    go build -v -a -tags netgo -ldflags '-w -extldflags "-static"' -o /go/bin/backup main.go

# now copy artifacts to a lightweight image
FROM scratch
COPY --from=builder /go/bin /bin
COPY --from=builder /etc/ssl/certs /etc/ssl/certs
ENTRYPOINT ["/bin/backup"]
