FROM golang:1.27 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /proxy .

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /proxy /proxy
ENV DB_PATH=/data/proxy.db LISTEN=:8080
VOLUME /data
EXPOSE 8080
ENTRYPOINT ["/proxy"]
