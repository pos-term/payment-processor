FROM golang:1.24-alpine AS build
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/payment-processor ./cmd/payment-processor

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/payment-processor /payment-processor
EXPOSE 8080
ENTRYPOINT ["/payment-processor"]
