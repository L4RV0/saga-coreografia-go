// Order Service: API para crear pedidos. Inicia la saga.
package main

import (
	"context"
	"errors"
	"net/http"

	"github.com/jackc/pgx/v5/pgxpool"

	"saga-coreografia/internal/broker"
	"saga-coreografia/internal/logx"
	"saga-coreografia/internal/order"
	"saga-coreografia/internal/platform"
)

func main() {
	platform.Run("order", logx.Blue, order.Schema, func(ctx context.Context, db *pgxpool.Pool, bus *broker.NATS, mux *http.ServeMux) error {
		svc := order.New(db, bus)
		if err := svc.Start(ctx); err != nil {
			return err
		}

		// POST /orders {"customer_id","product_id","quantity"} → 202 + pedido PENDING
		mux.HandleFunc("POST /orders", func(w http.ResponseWriter, r *http.Request) {
			var req struct {
				CustomerID string `json:"customer_id"`
				ProductID  string `json:"product_id"`
				Quantity   int    `json:"quantity"`
			}
			if err := platform.ReadJSON(r, &req); err != nil || req.CustomerID == "" {
				platform.Error(w, http.StatusBadRequest, "se requiere customer_id, product_id y quantity")
				return
			}
			o, err := svc.PlaceOrder(r.Context(), req.CustomerID, req.ProductID, req.Quantity)
			switch {
			case errors.Is(err, order.ErrUnknownProduct):
				platform.Error(w, http.StatusNotFound, err.Error())
			case err != nil:
				platform.Error(w, http.StatusBadRequest, err.Error())
			default:
				// 202: aceptado, la saga sigue en segundo plano (consistencia eventual).
				platform.WriteJSON(w, http.StatusAccepted, o)
			}
		})

		mux.HandleFunc("GET /orders/{id}", func(w http.ResponseWriter, r *http.Request) {
			o, err := svc.Get(r.Context(), r.PathValue("id"))
			if errors.Is(err, order.ErrNotFound) {
				platform.Error(w, http.StatusNotFound, err.Error())
				return
			}
			if err != nil {
				platform.Error(w, http.StatusInternalServerError, err.Error())
				return
			}
			platform.WriteJSON(w, http.StatusOK, o)
		})

		// POST /orders/{id}/cancel {"reason": "..."} → 202 + pedido CANCELLING.
		// Inicia las transacciones compensatorias (reembolso, stock, cancelación).
		mux.HandleFunc("POST /orders/{id}/cancel", func(w http.ResponseWriter, r *http.Request) {
			var req struct {
				Reason string `json:"reason"`
			}
			_ = platform.ReadJSON(r, &req) // el cuerpo es opcional
			o, err := svc.CancelOrder(r.Context(), r.PathValue("id"), req.Reason)
			switch {
			case errors.Is(err, order.ErrNotFound):
				platform.Error(w, http.StatusNotFound, err.Error())
			case errors.Is(err, order.ErrNotCancellable):
				platform.Error(w, http.StatusConflict, err.Error())
			case err != nil:
				platform.Error(w, http.StatusInternalServerError, err.Error())
			default:
				platform.WriteJSON(w, http.StatusAccepted, o)
			}
		})

		mux.HandleFunc("GET /orders", func(w http.ResponseWriter, r *http.Request) {
			list, err := svc.List(r.Context())
			if err != nil {
				platform.Error(w, http.StatusInternalServerError, err.Error())
				return
			}
			platform.WriteJSON(w, http.StatusOK, list)
		})

		// Administración: fijar precio de un producto del catálogo.
		mux.HandleFunc("PUT /admin/catalog/{product}", func(w http.ResponseWriter, r *http.Request) {
			var req struct {
				Price float64 `json:"price"`
			}
			if err := platform.ReadJSON(r, &req); err != nil {
				platform.Error(w, http.StatusBadRequest, "se requiere price")
				return
			}
			if err := svc.SetPrice(r.Context(), r.PathValue("product"), req.Price); err != nil {
				platform.Error(w, http.StatusBadRequest, err.Error())
				return
			}
			w.WriteHeader(http.StatusNoContent)
		})
		return nil
	})
}
