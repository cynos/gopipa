package pipa

import (
	"context"
	"fmt"
	"sync"
)

type Chan chan interface{}

type WorkerIndex int

type execFn func(ctx context.Context, s Stage, wi WorkerIndex)

type Stage interface {
	CloseData()
	GetStageID() string
	IsDataClosed() bool
	GetActiveWorker() int
	SetData(d interface{})
	GetData() Chan
	CatchPanic(func(error))
	Pipe() Pipeliner
	NextStage() Stage
	PrevStage() Stage
}

type worker struct {
	mutex    *sync.Mutex
	Numbers  int
	Active   int
	Position int
}

type stage struct {
	ID       string
	Worker   worker
	Data     Chan
	ExecFn   execFn
	pipeline *pipeline
}

func (s *stage) GetData() Chan {
	return s.Data
}

func (s *stage) SetData(d interface{}) {
	s.Data <- d
}

func (s *stage) SetActiveWorker(d int) {
	s.Worker.mutex.Lock()
	defer s.Worker.mutex.Unlock()
	if d >= 0 {
		s.Worker.Active++
	} else {
		s.Worker.Active--
	}
}

func (s *stage) GetActiveWorker() int {
	s.Worker.mutex.Lock()
	defer s.Worker.mutex.Unlock()
	return s.Worker.Active
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

func (s *stage) Pipe() Pipeliner {
	return s.pipeline
}

func (s *stage) NextStage() Stage {
	if len(s.pipeline.stages) > s.Worker.Position+1 {
		return s.pipeline.stages[s.Worker.Position+1]
	}
	return nil
}

func (s *stage) PrevStage() Stage {
	if l := len(s.pipeline.stages); l > 1 && s.Worker.Position > 0 {
		return s.pipeline.stages[s.Worker.Position-1]
	}
	return nil
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
	stages []*stage
}

func NewPipeline(ctx context.Context) Pipeliner {
	return &pipeline{
		ctx: ctx,
	}
}

func (p *pipeline) AddStage(id string, numWorkers int, fn execFn) {
	var chanData Chan
	if numWorkers <= 1 {
		chanData = make(Chan)
	} else {
		chanData = make(Chan, numWorkers)
	}
	p.stages = append(p.stages, &stage{
		ID:   id,
		Data: chanData,
		Worker: worker{
			mutex:   &sync.Mutex{},
			Numbers: numWorkers,
		},
		ExecFn:   fn,
		pipeline: p,
	})
}

func (p *pipeline) GetAllStage() []Stage {
	stages := []Stage{}
	for _, v := range p.stages {
		stages = append(stages, v)
	}
	return stages
}

func (p *pipeline) GetStageFrom(id string) Stage {
	for _, s := range p.stages {
		if s.ID == id {
			return s
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
	if len(p.stages) < 1 {
		panic("pipeline doesn't have any stages")
	}
	w := sync.WaitGroup{}
	for pos, s := range p.stages {
		s.Worker.Position = pos
		for i := 0; i < s.Worker.Numbers; i++ {
			w.Add(1)
			s.SetActiveWorker(1)
			go exec(p.ctx, &w, s, WorkerIndex(i))
		}
	}
	w.Wait()
	// make sure all channel closed when all stages is completed
	for _, v := range p.stages {
		v.CloseData()
	}
}

func exec(ctx context.Context, wg *sync.WaitGroup, s *stage, wi WorkerIndex) {
	defer wg.Done()
	defer s.SetActiveWorker(-1)
	s.ExecFn(ctx, s, wi)
}
