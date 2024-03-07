package database

import (
	"database/sql/driver"
	"fmt"

	"github.com/as-tool/as-etl-engine/common/config"
)

// Default Parameters
const (
	DefaultMaxOpenConns = 4
	DefaultMaxIdleConns = 4
)

// Source Data Source, containing driver information, package information, configuration files, and connection information
type Source interface {
	Config() *config.JSON   // Configuration Information
	Key() string            // Typically connection information
	DriverName() string     // Driver Name, used as the first parameter for sql.Open
	ConnectName() string    // Connection Information, used as the second parameter for sql.Open
	Table(*BaseTable) Table // Get Specific Table
}

// WithConnector Data Source with Connection, the data source prefers to call this method to generate a data connection pool DB
type WithConnector interface {
	Connector() (driver.Connector, error) // go 1.10 Get Connection
}

// NewSource Obtain the corresponding data source by the name of the database dialect
func NewSource(name string, conf *config.JSON) (source Source, err error) {
	d, ok := dialects.dialect(name)
	if !ok {
		return nil, fmt.Errorf("dialect %v does not exsit", name)
	}
	source, err = d.Source(NewBaseSource(conf))
	if err != nil {
		return nil, fmt.Errorf("dialect %v Source() err: %v", name, err)
	}
	return
}

// BaseSource Basic data source for storing JSON configuration files
// Used to embed Source, facilitating the implementation of various database Fields
type BaseSource struct {
	conf *config.JSON
}

// NewBaseSource Generate a basic data source from the JSON configuration file conf
func NewBaseSource(conf *config.JSON) *BaseSource {
	return &BaseSource{
		conf: conf.CloneConfig(),
	}
}

// Config Configuration file for the basic data source
func (b *BaseSource) Config() *config.JSON {
	return b.conf
}
