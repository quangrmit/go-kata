package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"

	"golang.org/x/sync/errgroup"
)

func shouldErr() bool {
	num := rand.Intn(11) // set error rate to be 30%
	return num <= 3

}

func main() {
	//  a question here is that can I achieve this without having to use channels and the select stmt
	/*
		    okay this will be my notes for the thought process
			let's sketch out what are the functional requirements

			both services are to be queried concurrently

			if either fails, the entire operation fails (fail-fast)

			if the global timeout reaches, the entire operation fails

			to use the timeout feature, we have to use the ctx.Done() API
			therefore, we need to use channels for this

			therefore, we need to use 2 channels for the 2 services

			okay now what is the problem in the current piece of code. the select stmt only runs once, so it can only evaluate 1 service's channel at a time. There fore, I think we need to apply the fan-in function, which combines 2 channels into 1.

	*/
	ua := &UserAggregator{}
	parentCtx := context.Background()
	ctx, cancel := context.WithTimeout(parentCtx, 5*time.Second)
	defer cancel()

	info := Info{}
	ci := make(chan Info)
	errCh := make(chan error, 1)

	go func() {

		err := ua.Aggregate(ctx, ci, 1)
		if err != nil {
			log.Fatal(err)
		}
		errCh <- err

	}()

	select {
	case info = <-ci:
		log.Println("received Info channel")
		log.Println(info)
	case <-errCh:
		log.Fatal("error with aggregate function")
	case <-ctx.Done():
		log.Fatal("context deadline exceeded")
	}
	log.Println("the end")
}

type UserAggregator struct {
}

func (ua *UserAggregator) Aggregate(context context.Context, ci chan Info, id int) error {
	var order = &Order{}
	os := &OrderService{}

	var profile = &Profile{}
	ps := &ProfileService{}

	eg := new(errgroup.Group)

	eg.Go(func() error {

		profile = ps.GetProfile()
		return nil
	})

	eg.Go(func() error {

		order = os.GetOrders()
		return nil
	})

	if err := eg.Wait(); err != nil {
		log.Fatal(err)
	}
	ci <- Info{
		profile: *profile,
		order:   *order,
	}
	return nil
}

type Order struct {
	Orders int
}
type OrderService struct {
}

func (os *OrderService) GetOrders() *Order {
	log.Println("beginning of GetOrders")
	//
	// if shouldErr() {
	// 	return errors.New("mock orders error")
	// }
	//
	// time.Sleep(6 * time.Second)
	//
	fmt.Println("get orders checkpoint")
	return &Order{Orders: 5}
}

type ProfileService struct {
}
type Profile struct {
	Name string
}

func (ps *ProfileService) GetProfile() *Profile {
	log.Println("beginning of GetProfile")
	// if shouldErr() {
	// 	return errors.New("mock profile error")
	// }
	// time.Sleep(6 * time.Second)
	return &Profile{Name: "Alice"}
}

type Info struct {
	profile Profile
	order   Order
}
