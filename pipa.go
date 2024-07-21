package pipa

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type Channel interface {
	Set(d interface{})
	Get(fn func(data interface{}))
	Close()
}

type channel struct {
	ID    int
	Stage *stage
}

func (c *channel) Set(d interface{}) {
	if c.Stage.Worker.Numbers > 1 {
		c.Stage.WorkerChannel[c.ID] <- d
	} else {
		c.Stage.MainChannel <- d
	}
}

func (c *channel) Get(fn func(data interface{})) {
	if prev := c.Stage.PrevStage(); prev != nil {
		for d := range prev.GetMainChannel() {
			fn(d)
		}
	} else {
		for d := range c.Stage.GetMainChannel() {
			fn(d)
		}
	}
}

func (c *channel) Close() {
	if c.Stage.Worker.Numbers > 1 {
		close(c.Stage.WorkerChannel[c.ID])
	} else {
		c.Stage.CloseData()
	}
}

type Stage interface {
	GetStageID() string
	GetActiveWorker() int
	GetMainChannel() chan interface{}
	CatchPanic(func(error))
	Pipe() Pipeliner
	NextStage() Stage
	PrevStage() Stage
}

type stageFn func(ctx context.Context, s Stage, ch Channel)

type stageWorker struct {
	sync.Mutex
	Numbers int
	Active  int
}

type stage struct {
	ID            string
	Index         int
	Worker        stageWorker
	WorkerChannel []chan interface{}
	MainChannel   chan interface{}
	ExecFn        stageFn
	Pipeline      *pipeline
}

func (s *stage) Channel(i int) Channel {
	return &channel{
		ID:    i,
		Stage: s,
	}
}

func (s *stage) GetMainChannel() chan interface{} {
	return s.MainChannel
}

func (s *stage) SetData(d interface{}) {
	s.MainChannel <- d
}

func (s *stage) CloseData() {
	close(s.MainChannel)
}

func (s *stage) IncreaseActiveWorker() {
	s.Worker.Lock()
	defer s.Worker.Unlock()
	s.Worker.Active++
}

func (s *stage) DecreaseActiveWorker() {
	s.Worker.Lock()
	defer s.Worker.Unlock()
	s.Worker.Active--
}

func (s *stage) GetActiveWorker() int {
	s.Worker.Lock()
	defer s.Worker.Unlock()
	return s.Worker.Active
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
	Start()
	Monitor()
	GetStageFrom(id string) Stage
	AddStage(id string, worker int, fn stageFn)
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
	if numWorkers < 1 {
		numWorkers = 1
	}

	var wch []chan interface{}
	for i := 0; i < numWorkers; i++ {
		wch = append(wch, make(chan interface{}))
	}

	var mch chan interface{}
	if numWorkers == 1 {
		mch = wch[0]
	} else {
		mch = make(chan interface{})
	}

	p.stages = append(p.stages, &stage{
		ID:            id,
		ExecFn:        fn,
		Pipeline:      p,
		Worker:        stageWorker{Numbers: numWorkers},
		MainChannel:   mch,
		WorkerChannel: wch,
	})
}

func (p *pipeline) GetStageFrom(id string) Stage {
	for _, s := range p.stages {
		if s.ID == id {
			return s
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
			go p.run(&w, s, i)
		}
		if len(s.WorkerChannel) > 1 {
			w2 := sync.WaitGroup{}
			w2.Add(len(s.WorkerChannel))
			for _, ch := range s.WorkerChannel {
				go func(w2 *sync.WaitGroup) {
					defer w2.Done()
					for d := range ch {
						s.SetData(d)
					}
				}(&w2)
			}
			go func() {
				w2.Wait()
				close(s.MainChannel)
			}()
		}
	}
	w.Wait()
}

func (p *pipeline) Monitor() {
	go func() {
		for {
			for i := 0; i < len(p.stages); i++ {
				stage := p.stages[i]
				fmt.Printf("pipeline [%s] \t worker numbers [%d] <> active [%d]\n", stage.ID, stage.Worker.Numbers, stage.GetActiveWorker())
			}
			fmt.Println("-----------------------")
			time.Sleep(time.Second)
		}
	}()
}

func (p *pipeline) run(wg *sync.WaitGroup, s *stage, workerIndex int) {
	defer wg.Done()
	defer s.DecreaseActiveWorker()
	s.IncreaseActiveWorker()
	s.ExecFn(p.ctx, s, s.Channel(workerIndex))
}
