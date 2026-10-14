FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY *.go ./
RUN CGO_ENABLED=0 go test -trimpath ./... && CGO_ENABLED=0 go build -trimpath -o /out/cube-j1-datadog .

FROM scratch
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /out/cube-j1-datadog /cube-j1-datadog
USER 1000:1000
ENTRYPOINT ["/cube-j1-datadog"]
