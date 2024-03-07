package dbms

import (
	"context"
	"time"

	coreconst "github.com/as-tool/as-etl-engine/common"
	"github.com/as-tool/as-etl-engine/common/config"
	"github.com/as-tool/as-etl-engine/core/plugin"

	"github.com/pingcap/errors"
)

// Job Work
type Job struct {
	*plugin.BaseJob

	Handler DbHandler // Database handle
	Execer  Execer    // Executor
	conf    Config    // Configuration
}

// NewJob Get work through database handle
func NewJob(handler DbHandler) *Job {
	return &Job{
		BaseJob: plugin.NewBaseJob(),
		Handler: handler,
	}
}

// Init Initialization
func (j *Job) Init(ctx context.Context) (err error) {
	var name string
	if name, err = j.PluginConf().GetString("dialect"); err != nil {
		return errors.Wrapf(err, "GetString fail")
	}

	if j.conf, err = j.Handler.Config(j.PluginJobConf()); err != nil {
		return errors.Wrapf(err, "Config fail")
	}

	var jobSettingConf *config.JSON
	if jobSettingConf, err = j.PluginJobConf().GetConfig(coreconst.DataxJobSetting); err != nil {
		jobSettingConf, _ = config.NewJSONFromString("{}")
		err = nil
	}
	jobSettingConf.Set("username", j.conf.GetUsername())
	jobSettingConf.Set("password", j.conf.GetPassword())
	jobSettingConf.Set("url", j.conf.GetURL())

	if j.Execer, err = j.Handler.Execer(name, jobSettingConf); err != nil {
		return errors.Wrapf(err, "Execer fail")
	}
	timeoutCtx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	err = j.Execer.PingContext(timeoutCtx)
	if err != nil {
		return errors.Wrapf(err, "PingContext fail")
	}
	return
}

// Prepare Preparation
func (j *Job) Prepare(ctx context.Context) (err error) {
	preSQL := j.conf.GetPreSQL()
	for _, v := range preSQL {
		select {
		case <-ctx.Done():
			return errors.Wrap(ctx.Err(), "canceled")
		default:
		}
		if _, err = j.Execer.ExecContext(ctx, v); err != nil {
			return errors.Wrapf(err, "ExecContext(%v) fail.", v)
		}
	}
	return
}

// Post Post-processing
func (j *Job) Post(ctx context.Context) (err error) {
	postSQL := j.conf.GetPostSQL()
	for _, v := range postSQL {
		select {
		case <-ctx.Done():
			return errors.Wrap(ctx.Err(), "canceled")
		default:
		}
		if _, err = j.Execer.ExecContext(ctx, v); err != nil {
			return errors.Wrapf(err, "ExecContext(%v) fail.", v)
		}
	}
	return
}

// Destroy Destruction
func (j *Job) Destroy(ctx context.Context) (err error) {
	if j.Execer != nil {
		err = j.Execer.Close()
	}
	return errors.Wrapf(err, "Close fail")
}

// Split Task division
func (j *Job) Split(ctx context.Context, number int) (confs []*config.JSON, err error) {
	for i := 0; i < number; i++ {
		confs = append(confs, j.PluginJobConf().CloneConfig())
	}
	return confs, nil
}
