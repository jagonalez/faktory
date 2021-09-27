package batch

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/contribsys/faktory/client"
	"github.com/contribsys/faktory/manager"
	"github.com/contribsys/faktory/server"

	//"github.com/contribsys/faktory/storage"
	"github.com/contribsys/faktory/util"
)

type BatchSubSystem struct {
	Server  *server.Server
	Batches map[string]*Batch
	mu      sync.Mutex
	Fetcher manager.Fetcher
}

type Lifecycle struct {
	BatchSubSystem *BatchSubSystem
}

type NewBatchRequest struct {
	ParentBid   string      `json:"parent_bid,omitempty"`
	Description string      `json:"description,omitempty"`
	Success     *client.Job `json:"success,omitempty"`
	Complete    *client.Job `json:"complete,omitempty"`
}

func Subsystem() *Lifecycle {
	return &Lifecycle{}
}

func (l *Lifecycle) Start(s *server.Server) error {
	l.BatchSubSystem = NewBatchSubSystem(s)
	l.BatchSubSystem.AddCommands()
	l.BatchSubSystem.AddMiddleware()
	return nil
}

func (l *Lifecycle) Name() string {
	return "Uniq"
}

func (l *Lifecycle) Reload(s *server.Server) error {
	l.BatchSubSystem.AddCommands()
	l.BatchSubSystem.LoadExistingBatches()
	return nil
}

func (l *Lifecycle) Shutdown(s *server.Server) error {
	return nil
}

func NewBatchSubSystem(s *server.Server) *BatchSubSystem {
	b := &BatchSubSystem{
		Server:  s,
		mu:      sync.Mutex{},
		Batches: make(map[string]*Batch),
		Fetcher: manager.BasicFetcher(s.Manager().Redis()),
	}
	b.LoadExistingBatches()
	return b

}

func (b *BatchSubSystem) AddCommands() {
	// BATCH NEW
	// BATCH STATUS
	// BATCH OPEN (only from within a bid)

	server.CommandSet["BATCH"] = func(c *server.Connection, s *server.Server, cmd string) {
		util.Info(fmt.Sprintf("cmd: %s", cmd))
		qs := strings.Split(cmd, " ")[1:]

		switch batchOperation := qs[0]; batchOperation {
		case "NEW":
			var batchRequest NewBatchRequest
			err := json.Unmarshal([]byte(qs[1]), &batchRequest)
			if err != nil {
				c.Error(cmd, fmt.Errorf("Invalid JSON data: %w", err))
			}

			batchId := fmt.Sprintf("b-%s", util.RandomJid())

			success, err := json.Marshal(batchRequest.Success)
			complete, err := json.Marshal(batchRequest.Complete)

			meta := b.NewBatchMeta(batchRequest.Description, batchRequest.ParentBid, string(success), string(complete))
			batch, err := b.NewBatch(batchId, meta)

			if err != nil {
				c.Error(cmd, fmt.Errorf("Unable to create batch: %w", err))
				return
			}

			c.Result([]byte(batch.Id))
		case "OPEN":
			batchId := qs[1]

			connection := reflect.ValueOf(*c)
			client := connection.FieldByName("client").Elem()

			wid := client.FieldByName("Wid").String()
			if wid == "" {
				c.Error(cmd, fmt.Errorf("Batches can only be opened from a worker processing a job"))
			}

			batch, err := b.GetBatch(batchId)
			if err != nil {
				c.Error(cmd, fmt.Errorf("Cannot find batch: %w", err))
			}
			_, ok := batch.Workers[wid]
			if !ok {
				c.Error(cmd, fmt.Errorf("This worker is not working on a job in the requested batch"))
			}

			c.Ok()
		case "COMMIT":
			batchId := qs[1]

			batch, err := b.GetBatch(batchId)
			if err != nil {
				c.Error(cmd, fmt.Errorf("Cannot find batch: %w", err))
			}

			batch.Commit()
			c.Ok()

		case "STATUS":
			c.Result([]byte("idunno"))
		default:
			c.Error(cmd, fmt.Errorf("Invalid BATCH operation %s", qs[0]))
		}

	}
}

func (b *BatchSubSystem) AddMiddleware() {
	b.Server.Manager().AddMiddleware("push", b.PushMiddleware)
	b.Server.Manager().SetFetcher(b)
	// b.Server.Manager().AddMiddleware("fetch", func(next func() error, ctx manager.Context) error {
	// 	if bid, ok := ctx.Job().GetCustom("bid"); ok {
	// 		// add job to batch and add worker id so it can re-open if needed
	// 		batch, err := b.GetBatch(bid.(string))
	// 		if err != nil {
	// 			util.Warnf("Unable to get batch: %w", err)
	// 			return next() // somethings wrong skip for now
	// 		}
	// 		println(ctx.Value("wid"))
	// 		println(fmt.Sprintf("%+v", ctx.Reservation()))
	// 		if ctx.Reservation().Wid == "" {
	// 			util.Info("Invalid worker")
	// 		} else {
	// 			batch.AddWorker(ctx.Reservation().Wid)
	// 			util.Infof("Added worker %s for job %s to %b", ctx.Reservation().Wid, ctx.Reservation().Wid, bid)
	// 		}
	// 	}
	// 	return next()
	// })
	b.Server.Manager().AddMiddleware("ack", func(next func() error, ctx manager.Context) error {
		if bid, ok := ctx.Job().GetCustom("bid"); ok {
			// add job to batch and add worker id so it can re-open if needed
			batch, err := b.GetBatch(bid.(string))
			if err != nil {
				util.Warnf("Unable to get batch: %w", err)
				return next() // somethings wrong skip for now
			}

			batch.JobFinished(ctx.Job().Jid, true)
			batch.RemoveWorker(ctx.Reservation().Wid)
			util.Infof("Job %s worker %s success for batch %s", ctx.Job().Jid, ctx.Reservation().Wid, bid)
		}
		return next()
	})
	b.Server.Manager().AddMiddleware("fail", func(next func() error, ctx manager.Context) error {
		if bid, ok := ctx.Job().GetCustom("bid"); ok {
			// add job to batch and add worker id so it can re-open if needed
			batch, err := b.GetBatch(bid.(string))
			if err != nil {
				util.Warnf("Unable to get batch: %w", err)
				return next() // somethings wrong skip for now
			}
			batch.JobFinished(ctx.Job().Jid, false)
			batch.RemoveWorker(ctx.Reservation().Wid)
			util.Infof("Job %s worker %s failed for batch %s", ctx.Job().Jid, ctx.Reservation().Wid, bid)
		}
		return next()
	})
	/*

		push:
			- check if job.custom.bid
			- check batch is open

			- push event to update bid data:
				- total + 1
				- pending + 1

		ack:
		    - check if job.custom.bid
			- check batch is open
			- event to update bid data <- do checks in there:
				- success + 1
			- event to see if success job should be pushed
			- event to see if completed job should be pushed

		fail:
		    - check if job.custom.bid
			- check batch is open
			- event to update bid data:
				- failed + 1
			- event to see if completed job should be pushed
	*/

}

func (b *BatchSubSystem) Fetch(ctx context.Context, wid string, queues ...string) (manager.Lease, error) {
	lease, err := b.Fetcher.Fetch(ctx, wid, queues...)
	if err == nil && lease != manager.Nothing {
		job, err := lease.Job()
		if err == nil && job != nil {
			if bid, ok := job.GetCustom("bid"); ok {
				// add job to batch and add worker id so it can re-open if needed
				batch, err := b.GetBatch(bid.(string))
				if err != nil {
					util.Warnf("Unable to get batch: %w", err)
				}
				batch.AddWorker(wid)
				util.Infof("Added worker %s for job %s to %b", wid, job.Jid, bid)
			}
		}

	}
	return lease, err
}

func (b *BatchSubSystem) PushMiddleware(next func() error, ctx manager.Context) error {
	if bid, ok := ctx.Job().GetCustom("bid"); ok {
		// add job to batch and add worker id so it can re-open if needed
		batch, err := b.GetBatch(bid.(string))
		if err != nil {
			util.Warnf("Unable to get batch: %w", err)
			return next() // somethings wrong skip for now
		}
		batch.JobQueued(ctx.Job().Jid)
		util.Infof("Added %s to batch %s", ctx.Job().Jid, batch.Id)
	}
	return next()
}

func (b *BatchSubSystem) LoadExistingBatches() error {
	vals, err := b.Server.Manager().Redis().SMembers("batches").Result()
	if err != nil {
		return err
	}
	for idx := range vals {

		batch, err := b.NewBatch(vals[idx], &BatchMeta{})
		if err != nil {
			util.Warnf("Unable to create batch: %v", err)
			continue
		}
		b.Batches[vals[idx]] = batch
	}
	return nil
}
func (b *BatchSubSystem) NewBatchMeta(description string, parentBid string, success string, complete string) *BatchMeta {
	return &BatchMeta{
		CreatedAt:   time.Now().UTC().Format(time.RFC3339Nano),
		Total:       0,
		Succeeded:   0,
		Failed:      0,
		Description: description,
		ParentBid:   parentBid,
		SuccessJob:  success,
		CompleteJob: complete,
	}
}

func (b *BatchSubSystem) NewBatch(batchId string, meta *BatchMeta) (*Batch, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	batch := &Batch{
		Id:          batchId,
		JobsKey:     fmt.Sprintf("jobs-%s", batchId),
		ChildrenKey: fmt.Sprintf("child-%s", batchId),
		MetaKey:     fmt.Sprintf("meta-%s", batchId),
		Meta:        meta,
		rclient:     b.Server.Manager().Redis(),
		mu:          sync.Mutex{},
		Workers:     make(map[string]bool),
		Server:      b.Server,
	}
	err := batch.Init()
	if err != nil {
		return nil, fmt.Errorf("Unable to initialize batch: %v", err)
	}
	err = b.Server.Manager().Redis().SAdd("batches", batchId).Err()
	if err != nil {
		return nil, fmt.Errorf("Unable to store batch: %v", err)
	}
	b.Batches[batchId] = batch

	return batch, nil

}

func (b *BatchSubSystem) GetBatch(batchId string) (*Batch, error) {
	if batchId == "" {
		return nil, fmt.Errorf("batchId cannot be blank")
	}

	batch, ok := b.Batches[batchId]

	if ok {
		return batch, nil
	}
	return nil, fmt.Errorf("No batch found")

}
