// Package logx imprime mensajes con el nombre y color de cada servicio para
// poder seguir la "coreografía" en la consola.
package logx

import (
	"fmt"
	"os"
	"sync"
)

const (
	Reset  = "\033[0m"
	Blue   = "\033[34m"
	Green  = "\033[32m"
	Purple = "\033[35m"
	Yellow = "\033[33m"
	Gray   = "\033[90m"
	Red    = "\033[31m"
	Bold   = "\033[1m"
)

var (
	mu      sync.Mutex
	enabled = true
	// NO_COLOR=1 desactiva los colores (útil para guardar logs en archivos).
	noColor = os.Getenv("NO_COLOR") != ""
)

func paint(color, s string) string {
	if noColor {
		return s
	}
	return color + s + Reset
}

// SetEnabled permite silenciar los logs (lo usan los tests).
func SetEnabled(v bool) {
	mu.Lock()
	enabled = v
	mu.Unlock()
}

// Log escribe "[servicio] mensaje" con color.
func Log(service, color, format string, args ...any) {
	mu.Lock()
	defer mu.Unlock()
	if !enabled {
		return
	}
	fmt.Fprintf(os.Stdout, "%s %s\n", paint(color, fmt.Sprintf("%-11s", "["+service+"]")), fmt.Sprintf(format, args...))
}

// Title imprime un encabezado.
func Title(format string, args ...any) {
	mu.Lock()
	defer mu.Unlock()
	if !enabled {
		return
	}
	fmt.Printf("\n%s\n", paint(Bold, fmt.Sprintf(format, args...)))
}
