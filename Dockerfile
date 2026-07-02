# One multi-stage image for all three pure-Go services (build arg SERVICE).
#   docker build --build-arg SERVICE=simulator -t fleet/simulator:local .
# simulator/ingest/query-api are CGO-free (duckdb lives only in analytics, which we don't deploy).
FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG SERVICE
RUN CGO_ENABLED=0 go build -trimpath -o /out/app ./${SERVICE}

FROM gcr.io/distroless/static-debian12
COPY --from=build /out/app /app
ENTRYPOINT ["/app"]
