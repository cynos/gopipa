package pipa

import (
	"context"
	"fmt"
	"sync"
)

type Stage interface {
	CloseData()
	GetStageID() string
	IsDataClosed() bool
	GetActiveWorker() int
	GetLenChan() int
	SetData(d interface{})
	SetDataFunc(func() interface{})
	GetData() chan interface{}
	CatchPanic(func(error))
	Pipe() Pipeliner
	NextStage() Stage
	PrevStage() Stage
}

type stageChan struct {
	sync.Mutex
	Data chan interface{}
}

type stageFn func(ctx context.Context, s Stage, workerIndex int)

type stageWorker struct {
	mutex   *sync.Mutex
	Numbers int
	Active  int
}

type stage struct {
	ID       string
	Index    int
	Worker   stageWorker
	Chan     stageChan
	ExecFn   stageFn
	Pipeline *pipeline
}

func (s *stage) GetData() chan interface{} {
	s.Chan.Lock()
	defer s.Chan.Unlock()
	return s.Chan.Data
}

func (s *stage) SetData(d interface{}) {
	s.Chan.Data <- d
}

func (s *stage) SetDataFunc(fn func() interface{}) {
	s.Chan.Lock()
	defer s.Chan.Unlock()
	if s.Chan.Data == nil {
		s.Chan.Data = make(chan interface{}, s.Worker.Numbers)
	}
	s.Chan.Data <- fn()
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

func (s *stage) GetLenChan() int {
	s.Chan.Lock()
	defer s.Chan.Unlock()
	return len(s.Chan.Data)
}

func (s *stage) CloseData() {
	if s.GetActiveWorker() == 1 {
		close(s.Chan.Data)
	}
}

func (s *stage) IsDataClosed() bool {
	select {
	case <-s.Chan.Data:
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
	return s.Pipeline
}

func (s *stage) NextStage() Stage {
	if len(s.Pipeline.stages) > s.Index+1 {
		return s.Pipeline.stages[s.Index+1]
	}
	return nil
}

func (s *stage) PrevStage() Stage {
	if l := len(s.Pipeline.stages); l > 1 && s.Index > 0 {
		return s.Pipeline.stages[s.Index-1]
	}
	return nil
}

type Pipeliner interface {
	AddStage(id string, worker int, fn stageFn)
	Start()
	GetDataStageFrom(id string) chan interface{}
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

func (p *pipeline) AddStage(id string, numWorkers int, fn stageFn) {
	p.stages = append(p.stages, &stage{
		ID:       id,
		ExecFn:   fn,
		Pipeline: p,
		Worker:   stageWorker{Numbers: numWorkers, mutex: &sync.Mutex{}},
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

func (p *pipeline) GetDataStageFrom(id string) chan interface{} {
	for _, s := range p.stages {
		if s.ID == id {
			return s.Chan.Data
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
		s.Index = pos
		for i := 0; i < s.Worker.Numbers; i++ {
			w.Add(1)
			s.SetActiveWorker(1)
			go p.run(&w, s, i)
		}
	}
	w.Wait()
	// make sure all channel closed when all stages is completed
	for _, v := range p.stages {
		v.CloseData()
	}
}

func (p *pipeline) run(wg *sync.WaitGroup, s *stage, workerIndex int) {
	defer wg.Done()
	defer s.SetActiveWorker(-1)
	defer s.CatchPanic(func(err error) {
		fmt.Println("Panic on worker stage", s.GetStageID(), "err", err.Error())
		if s.GetActiveWorker() == 1 {
			s.CloseData()
		}
	})
	s.ExecFn(p.ctx, s, workerIndex)
}
