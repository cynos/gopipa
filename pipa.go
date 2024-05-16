package pipa

import (
	"context"
	"fmt"
	"sync"
)

type Chan chan interface{}

type execFn func(ctx context.Context, s Stage)

type Stage interface {
	CloseData()
	GetStageID() string
	IsDataClosed() bool
	GetWorkerActiveCount() int
	SetData(d interface{})
	GetData() Chan
	CatchPanic(func(error))
}

type stage struct {
	ID           string
	Position     int
	WorkerCount  int
	WorkerActive int
	Data         Chan
	ExecFn       execFn
}

func (s *stage) GetData() Chan {
	return s.Data
}

func (s *stage) SetData(d interface{}) {
	s.Data <- d
}

func (s *stage) GetWorkerActiveCount() int {
	return s.WorkerActive
}

func (s *stage) CloseData() {
	if !s.IsDataClosed() {
		close(s.Data)
	}
}

func (s *stage) IsDataClosed() bool {
	select {
	case <-s.Data:
		return true
	default:
	}
	return false
}

func (s *stage) GetStageID() string {
	return s.ID
}

func (s *stage) CatchPanic(fn func(error)) {
	if r := recover(); r != nil {
		fn(fmt.Errorf("%v", r))
	}
}

type Pipeliner interface {
	AddStage(id string, worker int, fn execFn)
	Start()
	GetDataStageFrom(id string) Chan
	GetStageFrom(id string) Stage
	GetAllStage() []Stage
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

func (p *pipeline) GetAllStage() []Stage {
	stages := []Stage{}
	for _, v := range p.stages {
		stages = append(stages, &v)
	}
	return stages
}

func (p *pipeline) GetStageFrom(id string) Stage {
	for _, s := range p.stages {
		if s.ID == id {
			return &s
		}
	}
	return nil
}

func (p *pipeline) GetDataStageFrom(id string) Chan {
	for _, s := range p.stages {
		if s.ID == id {
			return s.Data
		}
	}
	return nil
}

func (p *pipeline) Start() {
	w := sync.WaitGroup{}
	for pos, s := range p.stages {
		s.Position = pos
		for i := 0; i < s.WorkerCount; i++ {
			w.Add(1)
			s.WorkerActive += 1
			go exec(p.ctx, &w, s)
		}
	}
	w.Wait()
	// make sure all channel closed when all stages is completed
	for _, v := range p.stages {
		v.CloseData()
	}
}

func exec(ctx context.Context, wg *sync.WaitGroup, s stage) {
	defer wg.Done()
	s.ExecFn(ctx, &s)
	s.WorkerActive -= 1
}
