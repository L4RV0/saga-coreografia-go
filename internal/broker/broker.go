// Package broker conecta los servicios con NATS JetStream, el "Message Broker
// System" del video. Los servicios NUNCA se llaman entre sí: publican eventos
// en un tópico y se suscriben a los tópicos de los demás.
//
// JetStream guarda los mensajes en disco (en su propio volumen de Docker) y
// los entrega "al menos una vez": si un servicio está caído, sus mensajes lo
// esperan; si falla al procesar uno, se le vuelve a entregar.
package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"saga-coreografia/internal/events"
	"saga-coreografia/internal/logx"
)

// StreamName es el stream de JetStream que almacena todos los eventos saga.*
const StreamName = "SAGA"

// Handler procesa un evento. Si devuelve error, el broker lo reintenta.
type Handler func(ctx context.Context, e events.Event) error

// Broker es lo único que los servicios conocen de la mensajería.
type Broker interface {
	Publish(ctx context.Context, topic string, e events.Event) error
	Subscribe(ctx context.Context, topic, durable string, h Handler) error
}

// NATS implementa Broker con NATS JetStream.
type NATS struct {
	service   string
	nc        *nats.Conn
	js        jetstream.JetStream
	stream    jetstream.Stream
	consumers []jetstream.ConsumeContext
}

// Connect se conecta a NATS (reintentando mientras el contenedor arranca) y
// crea el stream SAGA si no existe.
func Connect(ctx context.Context, url, service string) (*NATS, error) {
	var nc *nats.Conn
	var err error
	for i := 0; i < 30; i++ {
		nc, err = nats.Connect(url, nats.Name(service), nats.MaxReconnects(-1))
		if err == nil {
			break
		}
		logx.Log(service, logx.Gray, "esperando a NATS (%v)", err)
		time.Sleep(time.Second)
	}
	if err != nil {
		return nil, fmt.Errorf("conectando a NATS: %w", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		return nil, err
	}
	stream, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:       StreamName,
		Subjects:   []string{"saga.>"},
		Storage:    jetstream.FileStorage, // persistido en el volumen nats-data
		Duplicates: 2 * time.Minute,       // descarta publicaciones repetidas con el mismo ID
	})
	if err != nil {
		return nil, fmt.Errorf("creando stream: %w", err)
	}
	return &NATS{service: service, nc: nc, js: js, stream: stream}, nil
}

// Publish envía el evento al tópico. El ID del evento viaja como Nats-Msg-Id
// para que JetStream ignore una publicación duplicada.
func (b *NATS) Publish(ctx context.Context, topic string, e events.Event) error {
	data, err := json.Marshal(e)
	if err != nil {
		return err
	}
	_, err = b.js.Publish(ctx, topic, data, jetstream.WithMsgID(e.ID))
	return err
}

// Subscribe crea (o retoma) un consumidor durable: NATS recuerda hasta qué
// mensaje procesó este servicio, aunque el contenedor se reinicie.
func (b *NATS) Subscribe(ctx context.Context, topic, durable string, h Handler) error {
	cons, err := b.stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:       durable,
		FilterSubject: topic,
		AckPolicy:     jetstream.AckExplicitPolicy,
		MaxDeliver:    20,
	})
	if err != nil {
		return fmt.Errorf("creando consumidor %s: %w", durable, err)
	}

	cc, err := cons.Consume(func(msg jetstream.Msg) {
		var e events.Event
		if err := json.Unmarshal(msg.Data(), &e); err != nil {
			logx.Log(b.service, logx.Red, "mensaje inválido descartado: %v", err)
			_ = msg.Term()
			return
		}
		if err := h(ctx, e); err != nil {
			logx.Log(b.service, logx.Red, "error procesando %s (%s), se reintentará: %v", e.Type, e.OrderID, err)
			_ = msg.NakWithDelay(time.Second)
			return
		}
		_ = msg.Ack()
	})
	if err != nil {
		return err
	}
	b.consumers = append(b.consumers, cc)
	return nil
}

// Close detiene los consumidores y cierra la conexión.
func (b *NATS) Close() {
	for _, cc := range b.consumers {
		cc.Stop()
	}
	_ = b.nc.Drain()
}
