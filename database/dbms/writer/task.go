package dbms

import (
	"context"
	"time"

	"github.com/as-tool/as-etl-engine/core/spi/writer"

	"github.com/as-tool/as-etl-engine/common/config"

	"github.com/as-tool/as-etl-storage/database"

	coreconst "github.com/as-tool/as-etl-engine/common"
)

// Task
type Task struct {
	*writer.BaseTask

	Handler DbHandler
	Execer  Execer
	Config  Config
	Table   database.Table
}

// NewTask Create a task through the database handle
func NewTask(handler DbHandler) *Task {
	return &Task{
		BaseTask: writer.NewBaseTask(),
		Handler:  handler,
	}
}

// Init
func (t *Task) Init(ctx context.Context) (err error) {
	var name string
	if name, err = t.PluginConf().GetString("dialect"); err != nil {
		return t.Wrapf(err, "GetString fail")
	}

	if t.Config, err = t.Handler.Config(t.PluginJobConf()); err != nil {
		return t.Wrapf(err, "Config fail")
	}

	var jobSettingConf *config.JSON
	if jobSettingConf, err = t.PluginJobConf().GetConfig(coreconst.DataxJobSetting); err != nil {
		jobSettingConf, _ = config.NewJSONFromString("{}")
		err = nil
	}
	jobSettingConf.Set("username", t.Config.GetUsername())
	jobSettingConf.Set("password", t.Config.GetPassword())
	jobSettingConf.Set("url", t.Config.GetURL())

	if t.Execer, err = t.Handler.Execer(name, jobSettingConf); err != nil {
		return t.Wrapf(err, "Execer fail")
	}
	timeoutCtx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	err = t.Execer.PingContext(timeoutCtx)
	if err != nil {
		return t.Wrapf(err, "PingContext fail")
	}

	param := t.Handler.TableParam(t.Config, t.Execer)
	if setter, ok := param.Table().(database.ConfigSetter); ok {
		setter.SetConfig(t.PluginJobConf())
	}
	if t.Table, err = t.Execer.FetchTableWithParam(ctx, param); err != nil {
		return t.Wrapf(err, "FetchTableWithParam fail")
	}

	return
}

// Destroy
func (t *Task) Destroy(ctx context.Context) (err error) {
	if t.Execer != nil {
		err = t.Execer.Close()
	}
	return t.Wrapf(err, "Close fail")
}
