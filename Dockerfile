# WARNING: THIS FILE IS MANAGED IN THE 'BOILERPLATE' REPO AND COPIED TO OTHER REPOSITORIES.
# ONLY EDIT THIS FILE FROM WITHIN THE 'LYFT/BOILERPLATE' REPOSITORY:
# 
# TO OPT OUT OF UPDATES, SEE https://github.com/lyft/boilerplate/blob/master/Readme.rst

FROM --platform=${BUILDPLATFORM} golang:1.18-alpine3.16 as builder

ARG TARGETARCH
ENV GOARCH "${TARGETARCH}"
ENV GOOS linux

RUN apk add git openssh-client make curl

# COPY only the go mod files for efficient caching
COPY go.mod go.sum /go/src/github.com/flyteorg/flytepropeller/
WORKDIR /go/src/github.com/flyteorg/flytepropeller

# Pull dependencies
RUN go mod download

# COPY the rest of the source code
COPY . /go/src/github.com/flyteorg/flytepropeller/

# This 'linux_compile' target should compile binaries to the /artifacts directory
# The main entrypoint should be compiled to /artifacts/flytepropeller
RUN make linux_compile

# update the PATH to include the /artifacts directory
ENV PATH="/artifacts:${PATH}"

# This will eventually move to centurylink/ca-certs:latest for minimum possible image size
FROM alpine:3.16
LABEL org.opencontainers.image.source https://github.com/flyteorg/flytepropeller

COPY --from=builder /artifacts /bin

RUN apk --update add ca-certificates

RUN addgroup -S flyte && adduser -S flyte -G flyte
USER flyte

CMD ["flytepropeller"]
