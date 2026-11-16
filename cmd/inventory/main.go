// Inventory Service: reserva y libera stock reaccionando a eventos.
package main

import (
	"context"
	"net/http"

	"github.com/jackc/pgx/v5/pgxpool"

	"saga-coreografia/internal/broker"
	"saga-coreografia/internal/inventory"
	"saga-coreografia/internal/logx"
	"saga-coreografia/internal/platform"
)

func main() {
	platform.Run("inventory", logx.Green, inventory.Schema, func(ctx context.Context, db *pgxpool.Pool, bus *broker.NATS, mux *http.ServeMux) error {
		svc := inventory.New(db, bus)
		if err := svc.Start(ctx); err != nil {
			return err
		}

		mux.HandleFunc("GET /stock", func(w http.ResponseWriter, r *http.Request) {
			list, err := svc.List(r.Context())
			if err != nil {
				platform.Error(w, http.StatusInternalServerError, err.Error())
				return
			}
			platform.WriteJSON(w, http.StatusOK, list)
		})

		// Administración: fijar el stock de un producto.
		mux.HandleFunc("PUT /admin/stock/{product}", func(w http.ResponseWriter, r *http.Request) {
			var req struct {
				Stock int `json:"stock"`
			}
			if err := platform.ReadJSON(r, &req); err != nil {
				platform.Error(w, http.StatusBadRequest, "se requiere stock")
				return
			}
			if err := svc.SetStock(r.Context(), r.PathValue("product"), req.Stock); err != nil {
				platform.Error(w, http.StatusBadRequest, err.Error())
				return
			}
			w.WriteHeader(http.StatusNoContent)
		})
		return nil
	})
}
