// Package platform arranca un microservicio: conecta su base de datos y el
// broker, inicia el relay de la outbox y expone su API HTTP.
package platform

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"saga-coreografia/internal/broker"
	"saga-coreografia/internal/logx"
	"saga-coreografia/internal/store"
)

// Env lee una variable de entorno con valor por defecto.
func Env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// Setup prepara el servicio y devuelve sus rutas HTTP.
type Setup func(ctx context.Context, db *pgxpool.Pool, bus *broker.NATS, mux *http.ServeMux) error

// Run arranca el servicio y lo mantiene vivo hasta recibir SIGTERM.
func Run(service, color, schema string, setup Setup) {
	addr := Env("HTTP_ADDR", ":8080")

	// "app -healthcheck" lo usa Docker para saber si el servicio está sano.
	if len(os.Args) > 1 && os.Args[1] == "-healthcheck" {
		os.Exit(healthcheck(addr))
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	db, err := store.Open(ctx, Env("DATABASE_URL", ""), service, schema)
	if err != nil {
		fatal(service, err)
	}
	defer db.Close()

	bus, err := broker.Connect(ctx, Env("NATS_URL", "nats://localhost:4222"), service)
	if err != nil {
		fatal(service, err)
	}
	defer bus.Close()

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		WriteJSON(w, http.StatusOK, map[string]string{"status": "ok", "service": service})
	})
	if err := setup(ctx, db, bus, mux); err != nil {
		fatal(service, err)
	}

	go store.RunRelay(ctx, db, bus, service)

	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			fatal(service, err)
		}
	}()
	logx.Log(service, color, "listo · HTTP %s · base de datos y broker conectados", addr)

	<-ctx.Done()
	logx.Log(service, color, "apagando…")
	shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdown)
}

func healthcheck(addr string) int {
	c := http.Client{Timeout: 2 * time.Second}
	resp, err := c.Get("http://localhost" + addr + "/health")
	if err != nil || resp.StatusCode != http.StatusOK {
		return 1
	}
	return 0
}

func fatal(service string, err error) {
	logx.Log(service, logx.Red, "error fatal: %v", err)
	os.Exit(1)
}

// WriteJSON responde con JSON.
func WriteJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// ReadJSON lee el cuerpo JSON de la petición.
func ReadJSON(r *http.Request, v any) error {
	return json.NewDecoder(http.MaxBytesReader(nil, r.Body, 1<<20)).Decode(v)
}

// Error responde un error en JSON.
func Error(w http.ResponseWriter, status int, msg string) {
	WriteJSON(w, status, map[string]string{"error": msg})
}
