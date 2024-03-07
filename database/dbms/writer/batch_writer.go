package dbms

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/as-tool/as-etl-engine/core/plugin"

	"github.com/as-tool/as-etl-engine/common/element"
	"github.com/as-tool/as-etl-engine/module/schedule"
	"github.com/as-tool/as-etl-storage/database"

	"github.com/as-tool/as-etl-engine/module/transport/exchange"

	"github.com/pingcap/errors"
)

// Execution Mode
const (
	ExecModeNormal = "Normal" // Non-Transactional Execution
	ExecModeStmt   = "Stmt"   // prepare/exec without Transaction
	ExecModeTx     = "Tx"     // Transactional Execution
	ExecModeStmtTx = "StmtTx" // prepare/exec with Transaction
)

// BatchWriter - A tool or component used for writing data in batches.
type BatchWriter interface {
	JobID() int64                                                   // Job ID - A unique identifier for a job or task.
	TaskGroupID() int64                                             // Task Group ID - A unique identifier for a group of tasks.
	TaskID() int64                                                  // Task ID - A unique identifier for a specific task within a task group.
	BatchSize() int                                                 // Batch Size - The number of records to be written in a single batch.
	BatchTimeout() time.Duration                                    // Batch Timeout - The maximum time allowed for a single batch write operation.
	BatchWrite(ctx context.Context, records []element.Record) error // Batch Write - The process of writing data in batches.
}

// BaseBatchWriter - A basic implementation of a batch writer, providing the fundamental functionality for writing data in batches.
type BaseBatchWriter struct {
	Task     *Task
	execMode string
	strategy schedule.RetryStrategy
	judger   database.Judger
	opts     *database.ParameterOptions
}

// NewBaseBatchWriter - Creates a new instance of the basic batch writer based on the task, execution mode, and transaction options.
func NewBaseBatchWriter(task *Task, execMode string, opts *sql.TxOptions) *BaseBatchWriter {
	w := &BaseBatchWriter{
		Task:     task,
		execMode: execMode,
	}

	if j, ok := task.Table.(database.Judger); ok {
		strategy, err := task.Config.GetRetryStrategy(j)
		if err != nil {
			slog.Warn(fmt.Sprintf("[WARNING] jobID: %v taskgroupID:%v taskID: %v GetRetryStrategy fail error: %v",
				task.JobID(), task.TaskGroupID(), task.TaskID(), err))
		}
		w.strategy = strategy
		w.judger = j
	}

	if w.strategy == nil {
		w.strategy = schedule.NewNoneRetryStrategy()
	}

	w.opts = &database.ParameterOptions{
		Table:     task.Table,
		Mode:      task.Config.GetWriteMode(),
		TxOptions: opts,
	}
	return w
}

// JobID - The unique identifier for a job.
func (b *BaseBatchWriter) JobID() int64 {
	return b.Task.JobID()
}

// TaskGroupID - The unique identifier for a group of tasks.
func (b *BaseBatchWriter) TaskGroupID() int64 {
	return b.Task.TaskGroupID()
}

// TaskID - The unique identifier for a specific task within a task group.
func (b *BaseBatchWriter) TaskID() int64 {
	return b.Task.TaskID()
}

// BatchSize - The number of records to be inserted in a single batch.
func (b *BaseBatchWriter) BatchSize() int {
	return b.Task.Config.GetBatchSize()
}

// BatchTimeout - The maximum time allowed for a single batch insertion.
func (b *BaseBatchWriter) BatchTimeout() time.Duration {
	return b.Task.Config.GetBatchTimeout()
}

// BatchWrite - The process of writing data in batches.
func (b *BaseBatchWriter) BatchWrite(ctx context.Context, records []element.Record) (err error) {
	if b.strategy != nil {
		retry := schedule.NewRetryTask(ctx, b.strategy, newWriteTask(func() error {
			return b.batchWrite(ctx, records)
		}))
		err = retry.Do()
	}

	if b.judger != nil {
		if b.judger.ShouldOneByOne(err) {
			for _, r := range records {
				retry := schedule.NewRetryTask(ctx, b.strategy, newWriteTask(func() error {
					return b.batchWrite(ctx, []element.Record{r})
				}))
				err = retry.Do()
				if b.Task.Config.IgnoreOneByOneError() {
					err = nil
				}
			}
		}
	}
	return err
}

func (b *BaseBatchWriter) batchWrite(ctx context.Context, records []element.Record) error {
	b.opts.Records = records
	defer func() {
		b.opts.Records = nil
	}()
	switch b.execMode {
	case ExecModeTx:
		return b.Task.Execer.BatchExecWithTx(ctx, b.opts)
	case ExecModeStmt:
		return b.Task.Execer.BatchExecStmt(ctx, b.opts)
	case ExecModeStmtTx:
		return b.Task.Execer.BatchExecStmtWithTx(ctx, b.opts)
	}
	return b.Task.Execer.BatchExec(ctx, b.opts)
}

type writeTask struct {
	do func() error
}

func newWriteTask(do func() error) *writeTask {
	return &writeTask{
		do: do,
	}
}

func (t *writeTask) Do() error {
	return t.do()
}

// StartWrite - Begins the process of writing records to the database using the batch writer and record receiver.
func StartWrite(ctx context.Context, w BatchWriter,
	receiver plugin.RecordReceiver) (err error) {
	recordChan := make(chan element.Record)
	var rerr error
	afterCtx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	wg.Add(1)
	// Reads records from the record receiver and places them into the recordChan channel.
	go func() {
		defer func() {
			wg.Done()
			// Closes the recordChan channel.
			close(recordChan)
			slog.Debug(fmt.Sprintf("jobID: %v taskgroupID:%v taskID: %v get records end",
				w.JobID(), w.TaskGroupID(), w.TaskID()))
		}()
		slog.Debug(fmt.Sprintf("jobID: %v taskgroupID:%v taskID: %v start to get records",
			w.JobID(), w.TaskGroupID(), w.TaskID()))
		for {
			select {
			case <-afterCtx.Done():
				return
			default:
			}
			var record element.Record
			record, rerr = receiver.GetFromReader()
			if rerr != nil && rerr != exchange.ErrEmpty {
				return
			}
			// When the record receiver returns a non-empty error, it is written to the recordChan.
			if rerr != exchange.ErrEmpty {
				select {
				// Prevents records from not being written to the recordChan when the context (ctx) is closed.
				case <-afterCtx.Done():
					return
				case recordChan <- record:
				}
			}
		}
	}()
	ticker := time.NewTicker(w.BatchTimeout())
	defer ticker.Stop()
	var records []element.Record
	slog.Debug(fmt.Sprintf("jobID: %v taskgroupID:%v taskID: %v  start to BatchWrite",
		w.JobID(), w.TaskGroupID(), w.TaskID()))
	for {
		select {
		case record, ok := <-recordChan:
			if !ok {
				// Writes the remaining records to the database when the writing process ends.
				if len(records) > 0 {
					if err = w.BatchWrite(ctx, records); err != nil {
						slog.Error(fmt.Sprintf("jobID: %v taskgroupID:%v taskID: %v BatchWrite(%v) error: %+v",
							w.JobID(), w.TaskGroupID(), w.TaskID(), records, err))
					}
				}
				records = nil
				if err == nil {
					err = rerr
				}
				goto End
			}
			records = append(records, record)

			// Writes records to the database when the number of records exceeds the single batch size.
			if len(records) >= w.BatchSize() {
				if err = w.BatchWrite(ctx, records); err != nil {
					slog.Error(fmt.Sprintf("jobID: %v taskgroupID:%v taskID: %v BatchWrite(%v) error: %+v",
						w.JobID(), w.TaskGroupID(), w.TaskID(), records, err))
					goto End
				}
				records = nil
			}
		// Writes records to the database when the timeout is reached even if the number of records does not reach the single batch size.
		case <-ticker.C:
			if len(records) > 0 {
				if err = w.BatchWrite(ctx, records); err != nil {
					slog.Error(fmt.Sprintf("jobID: %v taskgroupID:%v taskID: %v BatchWrite(%v) error: %+v",
						w.JobID(), w.TaskGroupID(), w.TaskID(), records, err))
					goto End
				}
			}
			records = nil
		}
	}
End:
	cancel()
	slog.Debug(fmt.Sprintf("jobID: %v taskgroupID:%v taskID: %v wait all goroutine",
		w.JobID(), w.TaskGroupID(), w.TaskID()))
	// Waits for the goroutine to finish.
	wg.Wait()
	slog.Debug(fmt.Sprintf("jobID: %v taskgroupID:%v taskID: %v wait all goroutine end",
		w.JobID(), w.TaskGroupID(), w.TaskID()))
	switch {
	// Starting a write is not considered an error when externally canceled.
	case ctx.Err() != nil:
		return nil
	// Stopping due to an error is also not considered an error.
	case err == exchange.ErrTerminate:
		return nil
	}
	return errors.Wrapf(err, "jobID: %v taskgroupID:%v taskID: %v", w.JobID(), w.TaskGroupID(), w.TaskID())
}
