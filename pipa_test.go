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
	pipe.AddStage("numb", 1, func(ctx context.Context, s Stage, ch Channel) {
		for d := range s.NextStage().GetMainChannel() {
			fmt.Println(d)
		}
	})
	pipe.Start()
}

func TestPipeline(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	seeder := func(ctx context.Context, s Stage, ch Channel) {
		for i := 0; i < 10000; i++ {
			ch.Set(fmt.Sprintf("data#%d", i))
		}
		ch.Close()
	}

	consumer1 := func(ctx context.Context, s Stage, ch Channel) {
		ch.Get(func(data interface{}) {
			ch.Set(fmt.Sprintf("%s#modified", data.(string)))
		})
		ch.Close()
	}

	consumer2 := func(ctx context.Context, s Stage, ch Channel) {
		ch.Get(func(data interface{}) {
			database.Collection("pipeline").InsertOne(ctx, bson.D{bson.E{Key: "data", Value: data.(string)}})
		})
	}

	pipe := NewPipeline(ctx)
	pipe.AddStage("seeder", 1, seeder)
	pipe.AddStage("consumer1", 5, consumer1)
	pipe.AddStage("consumer2", 5, consumer2)
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
