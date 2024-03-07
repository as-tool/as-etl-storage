package database

import (
	"fmt"
	"sync"
)

// Dialect Database Dialect
type Dialect interface {
	Source(*BaseSource) (Source, error) // Data Source
}

var dialects = &dialectMap{
	dialects: make(map[string]Dialect),
}

// RegisterDialect Registers a database dialect. A panic occurs when the registered name is the same or the dialect is empty.
func RegisterDialect(name string, dialect Dialect) {
	if err := dialects.register(name, dialect); err != nil {
		panic(err)
	}
}

// UnregisterAllDialects Unregisters all database dialects.
func UnregisterAllDialects() {
	dialects.unregisterAll()
}

type dialectMap struct {
	sync.RWMutex
	dialects map[string]Dialect
}

func (d *dialectMap) register(name string, dialect Dialect) error {
	if dialect == nil {
		return fmt.Errorf("dialect %v is nil", name)
	}

	d.Lock()
	defer d.Unlock()
	if _, ok := d.dialects[name]; ok {
		return fmt.Errorf("dialect %v exists", name)
	}

	d.dialects[name] = dialect
	return nil
}

func (d *dialectMap) dialect(name string) (dialect Dialect, ok bool) {
	d.RLock()
	defer d.RUnlock()
	dialect, ok = d.dialects[name]
	return
}

func (d *dialectMap) unregisterAll() {
	d.Lock()
	defer d.Unlock()
	d.dialects = make(map[string]Dialect)
}
