# Una sola receta para los tres microservicios: SERVICE elige cuál compilar
# (order | inventory | payment).

# ---- Etapa 1: compilar ----
FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG SERVICE
RUN test -n "$SERVICE" && CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/app ./cmd/${SERVICE}

# ---- Etapa 2: imagen final mínima (sin shell, usuario sin privilegios) ----
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/app /app
USER nonroot:nonroot
EXPOSE 8080
HEALTHCHECK --interval=3s --timeout=3s --retries=20 CMD ["/app", "-healthcheck"]
ENTRYPOINT ["/app"]
