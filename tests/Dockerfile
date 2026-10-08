# syntax=docker/dockerfile:1
FROM golang:1.26 AS build
WORKDIR /src
COPY --from=gateway_source go.mod main.go main_test.go ./
RUN go test -race -timeout 60s ./...
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /gateway .

FROM python:3.14-slim
COPY --from=build /gateway /usr/local/bin/gateway
WORKDIR /work
COPY common.py fake_backend.py test_gateway.py test_live.py test_client.py ./
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 REPORT_DIR=/results
CMD ["python", "test_gateway.py"]
