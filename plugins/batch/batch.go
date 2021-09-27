package batch

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"

	//"github.com/contribsys/faktory/storage"

	"github.com/contribsys/faktory/client"
	"github.com/contribsys/faktory/server"
	"github.com/contribsys/faktory/util"
	"github.com/go-redis/redis"
)

type Batch struct {
	Id          string
	JobsKey     string
	ChildrenKey string
	MetaKey     string
	Children    map[string]*Batch
	Meta        *BatchMeta
	rclient     *redis.Client
	mu          sync.Mutex
	Workers     map[string]bool
	Server      *server.Server
}

type BatchMeta struct {
	ParentBid   string
	Total       int
	Failed      int
	Succeeded   int
	CreatedAt   string
	Description string
	Committed   bool
	SuccessJob  string
	CompleteJob string
}

func (b *Batch) Init() error {
	meta, err := b.rclient.HGetAll(b.MetaKey).Result()
	if err != nil {
		return nil
	}

	if len(meta) == 0 {
		data := map[string]interface{}{
			"total":        b.Meta.Total,
			"failed":       b.Meta.Failed,
			"succeeded":    b.Meta.Succeeded,
			"created_at":   b.Meta.CreatedAt,
			"description":  b.Meta.Description,
			"committed":    b.Meta.Committed,
			"parent_bid":   b.Meta.ParentBid,
			"success_job":  b.Meta.SuccessJob,
			"complete_job": b.Meta.CompleteJob,
		}
		b.rclient.HMSet(b.MetaKey, data)
		return nil
	}

	b.Meta.Total, err = strconv.Atoi(meta["total"])
	if err != nil {
		return err
	}

	b.Meta.Failed, err = strconv.Atoi(meta["failed"])
	if err != nil {
		return err
	}
	b.Meta.Succeeded, err = strconv.Atoi(meta["succeeded"])
	if err != nil {
		return err
	}
	b.Meta.Committed, err = strconv.ParseBool(meta["committed"])
	if err != nil {
		return err
	}
	b.Meta.Description = meta["description"]
	b.Meta.ParentBid = meta["parent_bid"]
	b.Meta.CreatedAt = meta["created_at"]
	b.Meta.SuccessJob = meta["success_job"]
	b.Meta.CompleteJob = meta["complete_job"]

	return nil
}

func (b *Batch) UpdateCommited(value bool) {
	b.Meta.Committed = value
	b.rclient.HSet(b.MetaKey, "commited", value)
	b.checkBatchDone()
}

func (b *Batch) AddJobToBatch(jobId string) {
	b.rclient.SAdd(b.JobsKey, jobId)
	b.rclient.HIncrBy(b.MetaKey, "total", 1)
}

func (b *Batch) RemoveJobFromBatch(jobId string, success bool) {
	b.rclient.SRem(b.JobsKey, jobId)
	if success {
		b.Meta.Succeeded += 1
		b.rclient.HIncrBy(b.MetaKey, "succeeded", 1)
	} else {
		b.Meta.Failed += 1
		b.rclient.HIncrBy(b.MetaKey, "failed", 1)
	}
	b.checkBatchDone()
}

func (b *Batch) hasParent() bool {
	return b.Meta.ParentBid == ""
}

func (b *Batch) isBatchDone() bool {
	totalFinished := b.Meta.Succeeded + b.Meta.Failed
	return b.Meta.Committed == true && totalFinished == b.Meta.Total
}

func (b *Batch) checkBatchDone() {
	if b.isBatchDone() {
		println(" WE DONE GOOD ")

		if b.Meta.Succeeded == b.Meta.Total && b.Meta.SuccessJob != "" {
			var succesJob client.Job
			err := json.Unmarshal([]byte(b.Meta.SuccessJob), &succesJob)
			if err != nil {
				util.Warnf("Cannot unmarshal success job %w", err)
			}
			succesJob.Jid = fmt.Sprintf("%s-success", b.Id)
			err = b.Server.Manager().Push(&succesJob)
			if err != nil {
				util.Warnf("Cannot push job %w", err)
			}
			util.Infof("Pushed job %+v", succesJob)
		}
		if b.Meta.CompleteJob != "" {
			var completeJob client.Job
			err := json.Unmarshal([]byte(b.Meta.CompleteJob), &completeJob)
			if err != nil {
				util.Warnf("Cannot unmarshal complete job %w", err)
			}
			completeJob.Jid = fmt.Sprintf("%s-complete", b.Id)
			err = b.Server.Manager().Push(&completeJob)
			if err != nil {
				util.Warnf("Cannot push job %w", err)
			}
			util.Infof("Pushed job %+v", completeJob)
		}
	}
}

func (b *Batch) isJobInBatch(jobId string) bool {
	return b.rclient.SIsMember(b.JobsKey, jobId).Val()
}

func (b *Batch) GetJobCountForBatch() int64 {
	return b.rclient.SCard(b.JobsKey).Val()
}

func (b *Batch) AddChildBatch(batch *Batch) {
	b.rclient.SAdd(b.ChildrenKey, batch.Id)
	b.Children[batch.Id] = batch
}

func (b *Batch) RemoveChildBatch(batchId string) error {
	err := b.rclient.SRem(b.ChildrenKey, batchId).Err()
	if err != nil {
		return err
	}
	delete(b.Children, batchId)
	return nil
}

func (b *Batch) Commit() {
	b.mu.Lock()
	b.UpdateCommited(true)
	b.mu.Unlock()
}

func (b *Batch) Open(requestingJobId string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if !b.isJobInBatch(requestingJobId) {
		return errors.New("Unable to open")
	}

	b.UpdateCommited(false)
	return nil
}

func (b *Batch) JobQueued(jobId string) {
	b.mu.Lock()
	b.AddJobToBatch(jobId)
	b.mu.Unlock()
}

func (b *Batch) JobFinished(jobId string, success bool) {
	b.mu.Lock()
	b.RemoveJobFromBatch(jobId, success)
	b.mu.Unlock()
}

func (b *Batch) AddWorker(wid string) {
	b.mu.Lock()
	b.Workers[wid] = true
	b.mu.Unlock()
}

func (b *Batch) RemoveWorker(wid string) {
	b.mu.Lock()
	delete(b.Workers, wid)
	b.mu.Unlock()
}
