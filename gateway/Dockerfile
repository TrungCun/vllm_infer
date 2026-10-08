FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod main.go main_test.go ./
RUN go test -race -timeout 60s ./...
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /gateway .

FROM scratch
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /gateway /gateway
USER 65532:65532
EXPOSE 8000
ENTRYPOINT ["/gateway"]
