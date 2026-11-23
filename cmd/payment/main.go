// Payment Service: cobra o rechaza reaccionando a eventos.
package main

import (
	"context"
	"errors"
	"net/http"
	"strconv"

	"github.com/jackc/pgx/v5/pgxpool"

	"saga-coreografia/internal/broker"
	"saga-coreografia/internal/logx"
	"saga-coreografia/internal/payment"
	"saga-coreografia/internal/platform"
)

func main() {
	platform.Run("payment", logx.Purple, payment.Schema, func(ctx context.Context, db *pgxpool.Pool, bus *broker.NATS, mux *http.ServeMux) error {
		svc := payment.New(db, bus)
		if err := svc.Start(ctx); err != nil {
			return err
		}

		mux.HandleFunc("GET /accounts", func(w http.ResponseWriter, r *http.Request) {
			list, err := svc.Accounts(r.Context())
			if err != nil {
				platform.Error(w, http.StatusInternalServerError, err.Error())
				return
			}
			platform.WriteJSON(w, http.StatusOK, list)
		})

		mux.HandleFunc("GET /payments/{order}", func(w http.ResponseWriter, r *http.Request) {
			p, err := svc.Get(r.Context(), r.PathValue("order"))
			if errors.Is(err, payment.ErrNotFound) {
				platform.Error(w, http.StatusNotFound, err.Error())
				return
			}
			if err != nil {
				platform.Error(w, http.StatusInternalServerError, err.Error())
				return
			}
			platform.WriteJSON(w, http.StatusOK, p)
		})

		// Movimientos de dinero: GET /account-movements?customer=...&order=...&limit=...
		mux.HandleFunc("GET /account-movements", func(w http.ResponseWriter, r *http.Request) {
			q := r.URL.Query()
			limit, _ := strconv.Atoi(q.Get("limit"))
			list, err := svc.Movements(r.Context(), q.Get("customer"), q.Get("order"), limit)
			if err != nil {
				platform.Error(w, http.StatusInternalServerError, err.Error())
				return
			}
			platform.WriteJSON(w, http.StatusOK, list)
		})

		// Administración: fijar el saldo de un cliente.
		mux.HandleFunc("PUT /admin/accounts/{customer}", func(w http.ResponseWriter, r *http.Request) {
			var req struct {
				Balance float64 `json:"balance"`
			}
			if err := platform.ReadJSON(r, &req); err != nil {
				platform.Error(w, http.StatusBadRequest, "se requiere balance")
				return
			}
			if err := svc.SetBalance(r.Context(), r.PathValue("customer"), req.Balance); err != nil {
				platform.Error(w, http.StatusBadRequest, err.Error())
				return
			}
			w.WriteHeader(http.StatusNoContent)
		})
		return nil
	})
}
