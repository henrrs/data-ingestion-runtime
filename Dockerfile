FROM golang:1.24 AS build

WORKDIR /src
COPY go.mod ./
COPY cmd ./cmd
COPY configs ./configs
COPY internal ./internal

RUN go build -o /out/landing-connector ./cmd/landing-connector

FROM gcr.io/distroless/base-debian12

WORKDIR /app
COPY --from=build /out/landing-connector /app/landing-connector

ENTRYPOINT ["/app/landing-connector"]
