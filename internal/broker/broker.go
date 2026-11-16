// Package broker implementa el "Message Broker System" del video: un sistema
// de publicación/suscripción por tópicos. Los servicios NUNCA se llaman entre
// sí directamente; solo publican eventos y reaccionan a los eventos de otros.
//
// Esta versión vive en memoria (goroutines + canales) para que el proyecto
// corra sin instalar nada. Como los servicios dependen de la interfaz Broker,
// se puede cambiar por Kafka, RabbitMQ o NATS sin tocar su lógica.
package broker

import (
	"sync"

	"saga-coreografia/internal/events"
	"saga-coreografia/internal/logx"
)

// Handler procesa un evento recibido.
type Handler func(events.Event)

// Broker es lo único que los servicios conocen del sistema de mensajería.
type Broker interface {
	Publish(topic string, e events.Event)
	Subscribe(topic, subscriber string, h Handler)
}

type subscription struct {
	name string
	ch   chan events.Event
}

// InMemory es un broker asíncrono: Publish no espera a que los consumidores
// procesen el evento (igual que un broker real).
type InMemory struct {
	mu      sync.RWMutex
	subs    map[string][]*subscription
	pending sync.WaitGroup // eventos publicados que aún no terminan de procesarse
}

// NewInMemory crea un broker vacío.
func NewInMemory() *InMemory {
	return &InMemory{subs: make(map[string][]*subscription)}
}

// Subscribe registra un consumidor en un tópico. Cada suscripción tiene su
// propia cola (canal) y su propia goroutine, como un consumer group.
func (b *InMemory) Subscribe(topic, subscriber string, h Handler) {
	s := &subscription{name: subscriber, ch: make(chan events.Event, 256)}

	b.mu.Lock()
	b.subs[topic] = append(b.subs[topic], s)
	b.mu.Unlock()

	go func() {
		for e := range s.ch {
			h(e)
			b.pending.Done()
		}
	}()
}

// Publish entrega una copia del evento a cada suscriptor del tópico.
func (b *InMemory) Publish(topic string, e events.Event) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	logx.Log("broker", logx.Gray, "📨 %-17s → %s", e.Type, topic)
	for _, s := range b.subs[topic] {
		b.pending.Add(1)
		s.ch <- e
	}
}

// WaitIdle bloquea hasta que todos los eventos publicados (y los que estos
// provocaron en cadena) fueron procesados. Sirve para la demo y los tests.
func (b *InMemory) WaitIdle() { b.pending.Wait() }

// Close cierra todas las colas.
func (b *InMemory) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, list := range b.subs {
		for _, s := range list {
			close(s.ch)
		}
	}
	b.subs = map[string][]*subscription{}
}
