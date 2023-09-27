FROM docker.io/library/golang:1.20 AS builder

WORKDIR /revenue

# get dependencies
COPY go.mod go.sum ./
RUN go mod download

# copy source
COPY . .

# codegen
RUN go generate ./...

# build
RUN go build -o bin/ -tags='netgo timetzdata' -trimpath -a -ldflags '-s -w -linkmode external -extldflags "-static"'  ./cmd/revenued

FROM docker.io/library/alpine:3
LABEL maintainer="The Sia Foundation <info@sia.tech>" \
      org.opencontainers.image.description.vendor="The Sia Foundation" \
      org.opencontainers.image.description="A Sia supply API - provides the Siacoin supply in the format specified by  statistics for the Sia network" \
      org.opencontainers.image.source="https://github.com/SiaFoundation/contract-revenue" \
      org.opencontainers.image.licenses=MIT

ENV PUID=0
ENV PGID=0

# copy binary and prepare data dir.
COPY --from=builder /revenue/bin/* /usr/bin/
VOLUME [ "/data" ]

# API port
EXPOSE 9980/tcp
# RPC port
EXPOSE 9981/tcp

USER ${PUID}:${PGID}

ENTRYPOINT [ "revenued", "-dir", "/data"]