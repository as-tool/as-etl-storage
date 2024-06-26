// Copyright 2020 the go-etl Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mysql

import (
	"encoding/json"

	"github.com/as-tool/as-etl-engine/common/config"
	"github.com/go-sql-driver/mysql"
)

// Config is the MySQL configuration, and the reading time needs to be parsed, i.e., parseTime=true
type Config struct {
	URL      string `json:"url"`      // Database URL, including the database address and other database parameters
	Username string `json:"username"` // Username
	Password string `json:"password"` // Password
}

// NewConfig creates a MySQL configuration and will report an error if the format does not meet the requirements
func NewConfig(conf *config.JSON) (c *Config, err error) {
	c = &Config{}
	err = json.Unmarshal([]byte(conf.String()), c)
	if err != nil {
		return nil, err
	}
	return
}

// FetchMysqlConfig retrieves the generated MySQL connection configuration, and will report an error if the URL is incorrect
func (c *Config) FetchMysqlConfig() (conf *mysql.Config, err error) {
	if conf, err = mysql.ParseDSN(c.URL); err != nil {
		return
	}
	conf.User = c.Username
	conf.Passwd = c.Password
	conf.ParseTime = true
	return
}
