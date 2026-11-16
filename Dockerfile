# ---- Etapa 1: compilar y probar ----
FROM golang:1.22-alpine AS build
WORKDIR /src

COPY go.mod ./
RUN go mod download

COPY . .
# Si una prueba falla, la imagen no se construye.
RUN go vet ./... && go test ./...
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/saga .

# ---- Etapa 2: imagen final mínima ----
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/saga /saga
USER nonroot:nonroot
ENTRYPOINT ["/saga"]
