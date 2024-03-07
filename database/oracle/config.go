package oracle

import (
	"encoding/json"

	"github.com/as-tool/as-etl-engine/common/config"

	"github.com/godror/godror"
)

type Config struct {
	URL      string `json:"url"`      // Database URL, including the database address and other database parameters
	Username string `json:"username"` // Username
	Password string `json:"password"` // Password
}

// NewConfig creates an Oracle configuration and will report an error if the format does not meet the requirements
func NewConfig(conf *config.JSON) (c *Config, err error) {
	c = &Config{}
	err = json.Unmarshal([]byte(conf.String()), c)
	if err != nil {
		return nil, err
	}
	return
}

// FetchConnectionParams retrieves the Oracle connection parameters and will report an error if the URL is incorrect
func (c *Config) FetchConnectionParams() (con godror.ConnectionParams, err error) {
	if con, err = godror.ParseDSN(c.URL); err != nil {
		return
	}
	con.Username = c.Username
	con.Password = godror.NewPassword(c.Password)
	return
}
