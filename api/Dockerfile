# Builds the API, worker, migrate, and keygen binaries into one minimal image.
#
# All four ship together because they share internal/ and are versioned as one
# unit: a deploy that ran migrations from a different build than the server is
# exactly the mismatch that causes a broken release.

# ---- build ----
FROM golang:1.25-alpine AS build

WORKDIR /src

# Dependencies are copied and downloaded first, so this layer is cached and
# reused for every change that does not touch go.mod or go.sum.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# CGO_ENABLED=0 produces a static binary, which is what allows the final stage
# to be a scratch-like base with no libc at all.
# -trimpath strips local filesystem paths; -s -w drop the symbol and DWARF
# tables, which are useless in production and a meaningful chunk of the size.
ARG VERSION=dev
RUN CGO_ENABLED=0 GOOS=linux go build \
      -trimpath \
      -ldflags="-s -w -X main.version=${VERSION}" \
      -o /out/ \
      ./cmd/server ./cmd/worker ./cmd/migrate ./cmd/keygen

# ---- runtime ----
# distroless: no shell, no package manager, no busybox. Nothing for an attacker
# who achieves code execution to pivot with, and nothing to patch for CVEs in
# utilities this service never uses.
FROM gcr.io/distroless/static-debian12:nonroot

# The migrations must ship with the binary that expects them, or a deploy can
# run one version's migrate against another version's schema.
COPY --from=build /src/migrations /migrations
COPY --from=build /out/ /usr/local/bin/

# Runs as uid 65532, never root: a container escape starts from an unprivileged
# account rather than a privileged one.
USER nonroot:nonroot

WORKDIR /

EXPOSE 8080

# The server is the default; the other binaries are reached by overriding the
# command, e.g. `docker run … worker` or `docker run … migrate -status`.
ENTRYPOINT ["/usr/local/bin/server"]
