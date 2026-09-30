# Builds the benchmark harness (api/cmd/bench). Kept separate from the service
# image so the load generator never ships in production.
FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/bench ./cmd/bench

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/bench /usr/local/bin/bench
ENTRYPOINT ["/usr/local/bin/bench"]
