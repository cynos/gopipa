package pipa

import (
	"context"
	"log"
	"sync"
)

type Chan chan interface{}

type execFn func(ctx context.Context, s Stage)

type Stage interface {
	CloseData()
	GetStageID() string
	GetWorkerindex() int
	IsDataClosed() bool
	SetData(d interface{})
}

type stage struct {
	ID          string
	WorkerIndex int
	WorkerCount int
	Data        Chan
	ExecFn      execFn
}

func (s stage) SetData(d interface{}) {
	s.Data <- d
}

func (s stage) CloseData() {
	close(s.Data)
}

func (s stage) IsDataClosed() bool {
	select {
	case <-s.Data:
		return true
	default:
	}
	return false
}

func (s stage) GetWorkerindex() int {
	return s.WorkerIndex
}

func (s stage) GetStageID() string {
	return s.ID
}

type Pipeliner interface {
	AddStage(id string, worker int, fn execFn)
	Start()
	GetDataFromStage(id string) Chan
}

type pipeline struct {
	ctx    context.Context
	stages []stage
}

func NewPipeline(ctx context.Context) Pipeliner {
	return &pipeline{
		ctx: ctx,
	}
}

func (p *pipeline) AddStage(id string, worker int, fn execFn) {
	var chanData Chan
	if worker <= 1 {
		chanData = make(Chan)
	} else {
		chanData = make(Chan, worker)
	}
	p.stages = append(p.stages, stage{
		ID:          id,
		Data:        chanData,
		WorkerCount: worker,
		ExecFn:      fn,
	})
}

func (p *pipeline) GetDataFromStage(id string) Chan {
	for _, s := range p.stages {
		if s.ID == id {
			return s.Data
		}
	}
	return nil
}

func (p *pipeline) Start() {
	wg := sync.WaitGroup{}
	for _, v := range p.stages {
		log.Println("running stage :", v.ID)
		wg.Add(1)
		go func(s stage) {
			defer wg.Done()
			defer log.Println("closing stage :", s.ID)
			w2 := sync.WaitGroup{}
			for i := 0; i < s.WorkerCount; i++ {
				w2.Add(1)
				s.WorkerIndex = i
				go exec(p.ctx, &w2, s)
			}
			w2.Wait()
			s.CloseData()
		}(v)
	}
	wg.Wait()
}

func exec(ctx context.Context, wg *sync.WaitGroup, s stage) {
	defer wg.Done()
	s.ExecFn(ctx, s)
}
