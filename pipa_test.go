package pipa

import (
	"context"
	"fmt"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

func InitMongoDB(ctx context.Context) (*mongo.Client, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	opts := options.Client().
		ApplyURI("mongodb://localhost:27017").
		SetServerAPIOptions(options.ServerAPI(options.ServerAPIVersion1))

	client, err := mongo.Connect(ctx, opts)
	if err != nil {
		return nil, err
	}

	if err := client.Ping(ctx, readpref.Primary()); err != nil {
		return nil, err
	}

	return client, nil
}

var database *mongo.Database

func init() {
	client, err := InitMongoDB(context.Background())
	if err != nil {
		panic(fmt.Sprintf("failed to connect mongo, Err : %s", err.Error()))
	}
	database = client.Database("test")
}

func TestCloseChannel(t *testing.T) {
	jobs := make(chan int, 5)
	done := make(chan bool)

	fmt.Println("cap :", cap(jobs), "len :", len(jobs))

	go func() {
		for {
			j, more := <-jobs
			if more {
				fmt.Println("received job", j)
			} else {
				fmt.Println("received all jobs")
				done <- true
				return
			}
		}
	}()

	for j := 1; j <= 3; j++ {
		jobs <- j
		fmt.Println("sent job", j)
	}
	// close(jobs)
	jobs = nil
	fmt.Println("cap :", cap(jobs), "len :", len(jobs))
	fmt.Println("sent all jobs")

	<-done

	_, ok := <-jobs
	fmt.Println("received more jobs:", ok)
}

func TestEmptyStage(t *testing.T) {
	defer func() {
		r := recover()
		if r != nil {
			t.Log(r)
		} else {
			t.Error("pipeline working correctly")
		}
	}()
	pipe := NewPipeline(context.Background())
	pipe.Start()
}

func TestEmptyNextStage(t *testing.T) {
	defer func() {
		r := recover()
		if r != nil {
			t.Log(r)
		} else {
			t.Error("pipeline working correctly")
		}
	}()
	pipe := NewPipeline(context.Background())
	pipe.AddStage("numb", 1, func(ctx context.Context, s Stage, wi int) {
		for d := range s.NextStage().GetData() {
			fmt.Println(d)
		}
	})
	pipe.Start()
}

func TestPipeline(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pipe := NewPipeline(ctx)
	pipe.AddStage("seeder", 1, func(ctx context.Context, s Stage, wi int) {
		for i := 0; i < 1000; i++ {
			s.SetDataFunc(func() interface{} {
				return fmt.Sprintf("data#%d", i)
			})
		}
		s.CloseData()
	})
	pipe.AddStage("consumer1", 5, func(ctx context.Context, s Stage, wi int) {
		for d := range s.PrevStage().GetData() {
			s.SetDataFunc(func() interface{} {
				return fmt.Sprintf("%s#modified", d.(string))
			})
		}
		s.CloseData()
	})
	pipe.AddStage("consumer2", 3, func(ctx context.Context, s Stage, wi int) {
		for d := range s.PrevStage().GetData() {
			database.Collection("pipeline").InsertOne(ctx, bson.D{bson.E{Key: "data", Value: d.(string)}})
		}
	})
	// go func() {
	// 	for {
	// 		seeder := pipe.GetStageFrom("seeder")
	// 		consumer1 := pipe.GetStageFrom("consumer1")
	// 		consumer2 := pipe.GetStageFrom("consumer2")
	// 		fmt.Printf("pipeline [seeder] have %d worker, channel length %d\n", seeder.GetActiveWorker(), seeder.GetLenChan())
	// 		fmt.Printf("pipeline [consumer1] have %d worker, channel length %d\n", consumer1.GetActiveWorker(), consumer1.GetLenChan())
	// 		fmt.Printf("pipeline [consumer2] have %d worker, channel length %d\n", consumer2.GetActiveWorker(), consumer2.GetLenChan())
	// 		time.Sleep(time.Second)
	// 	}
	// }()
	pipe.Start()
}

func TestManualLoop(t *testing.T) {
	var data = []string{}
	for i := 0; i < 10000; i++ {
		data = append(data, fmt.Sprintf("data#%d", i))
	}
	for i, v := range data {
		v = fmt.Sprintf("%s#modified", v)
		data[i] = v
	}
	for _, v := range data {
		database.Collection("pipeline").InsertOne(context.Background(), bson.D{bson.E{Key: "data", Value: v}})
	}
}
