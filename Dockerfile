# Start from a Debian image with the latest version of Go installed
# and a workspace (GOPATH) configured at /go.
ARG REGISTRY="docker.io/library"
ARG BASE_IMAGE=golang
ARG BASE_TAG=1.26-trixie@sha256:e6e8ff4b72b128bb673613645c5ac415e4f537b2390e77a86ffc40622ab56da8

FROM $REGISTRY/$BASE_IMAGE:$BASE_TAG AS builder
ENV DEBIAN_FRONTEND=noninteractive
ENV GOOS=linux GOARCH=amd64 CGO_ENABLED=0 GO111MODULE=on GOPATH=/src/tmp/go
ARG XDG_CONFIG_HOME

RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive \
    apt-get upgrade -y

RUN git config --global url."git@github.com:AustralianCyberSecurityCentre/".insteadOf "https://github.com/AustralianCyberSecurityCentre/"

# copy somewhere outside GOPATH/src AS using go modules
COPY . /src

# full static builds with no ld deps, so we can copy it to scratch
RUN cd /src && go build -v -a -tags netgo -ldflags '-w -extldflags "-static"' -o /go/bin/azul-recovery main.go

# now copy artifacts to a lightweight image
FROM scratch
COPY --from=builder /go/bin /bin
COPY --from=builder /etc/ssl/certs /etc/ssl/certs
ENTRYPOINT ["/bin/azul-recovery"]
