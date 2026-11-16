// Package client llama a las APIs HTTP de los tres servicios. Lo usan la demo
// (main.go) y las pruebas de extremo a extremo (e2e/).
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"saga-coreografia/internal/inventory"
	"saga-coreografia/internal/order"
	"saga-coreografia/internal/payment"
	"saga-coreografia/internal/platform"
)

var ErrNotFound = errors.New("no encontrado")

type Client struct {
	OrderURL, InventoryURL, PaymentURL string
	http                               *http.Client
}

// FromEnv usa ORDER_URL, INVENTORY_URL y PAYMENT_URL (por defecto, los
// puertos que publica docker-compose).
func FromEnv() *Client {
	return &Client{
		OrderURL:     platform.Env("ORDER_URL", "http://localhost:8081"),
		InventoryURL: platform.Env("INVENTORY_URL", "http://localhost:8082"),
		PaymentURL:   platform.Env("PAYMENT_URL", "http://localhost:8083"),
		http:         &http.Client{Timeout: 5 * time.Second},
	}
}

// WaitReady espera a que los tres servicios respondan /health.
func (c *Client) WaitReady(ctx context.Context, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for _, base := range []string{c.OrderURL, c.InventoryURL, c.PaymentURL} {
		for {
			if err := c.do(ctx, http.MethodGet, base+"/health", nil, nil); err == nil {
				break
			} else if time.Now().After(deadline) {
				return fmt.Errorf("%s no responde: %w (¿ejecutaste 'docker compose up -d'?)", base, err)
			}
			time.Sleep(500 * time.Millisecond)
		}
	}
	return nil
}

func (c *Client) PlaceOrder(ctx context.Context, customer, product string, qty int) (order.Order, error) {
	var o order.Order
	err := c.do(ctx, http.MethodPost, c.OrderURL+"/orders",
		map[string]any{"customer_id": customer, "product_id": product, "quantity": qty}, &o)
	return o, err
}

func (c *Client) GetOrder(ctx context.Context, id string) (order.Order, error) {
	var o order.Order
	err := c.do(ctx, http.MethodGet, c.OrderURL+"/orders/"+url.PathEscape(id), nil, &o)
	return o, err
}

// WaitFinal espera a que el pedido salga de PENDING.
func (c *Client) WaitFinal(ctx context.Context, id string, timeout time.Duration) (order.Order, error) {
	deadline := time.Now().Add(timeout)
	for {
		o, err := c.GetOrder(ctx, id)
		if err == nil && o.Status != order.Pending {
			return o, nil
		}
		if time.Now().After(deadline) {
			return o, fmt.Errorf("el pedido %s sigue en %s después de %s", id, o.Status, timeout)
		}
		time.Sleep(150 * time.Millisecond)
	}
}

func (c *Client) Stock(ctx context.Context, product string) (int, error) {
	var list []inventory.Product
	if err := c.do(ctx, http.MethodGet, c.InventoryURL+"/stock", nil, &list); err != nil {
		return 0, err
	}
	for _, p := range list {
		if p.ProductID == product {
			return p.Stock, nil
		}
	}
	return 0, ErrNotFound
}

func (c *Client) Balance(ctx context.Context, customer string) (float64, error) {
	var list []payment.Account
	if err := c.do(ctx, http.MethodGet, c.PaymentURL+"/accounts", nil, &list); err != nil {
		return 0, err
	}
	for _, a := range list {
		if a.CustomerID == customer {
			return a.Balance, nil
		}
	}
	return 0, ErrNotFound
}

func (c *Client) GetPayment(ctx context.Context, orderID string) (payment.Payment, error) {
	var p payment.Payment
	err := c.do(ctx, http.MethodGet, c.PaymentURL+"/payments/"+url.PathEscape(orderID), nil, &p)
	return p, err
}

func (c *Client) SetPrice(ctx context.Context, product string, price float64) error {
	return c.do(ctx, http.MethodPut, c.OrderURL+"/admin/catalog/"+url.PathEscape(product), map[string]any{"price": price}, nil)
}

func (c *Client) SetStock(ctx context.Context, product string, stock int) error {
	return c.do(ctx, http.MethodPut, c.InventoryURL+"/admin/stock/"+url.PathEscape(product), map[string]any{"stock": stock}, nil)
}

func (c *Client) SetBalance(ctx context.Context, customer string, balance float64) error {
	return c.do(ctx, http.MethodPut, c.PaymentURL+"/admin/accounts/"+url.PathEscape(customer), map[string]any{"balance": balance}, nil)
}

func (c *Client) do(ctx context.Context, method, u string, body, out any) error {
	var rd *bytes.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		rd = bytes.NewReader(b)
	} else {
		rd = bytes.NewReader(nil)
	}
	req, err := http.NewRequestWithContext(ctx, method, u, rd)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return ErrNotFound
	}
	if resp.StatusCode >= 300 {
		var e struct {
			Error string `json:"error"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&e)
		return fmt.Errorf("%s %s: %d %s", method, u, resp.StatusCode, e.Error)
	}
	if out != nil {
		return json.NewDecoder(resp.Body).Decode(out)
	}
	return nil
}
