package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"time"

	"golang.org/x/sync/errgroup"
)

const GLOBAL_TIMEOUT = 1
const SERVICE_1_TIMEOUT = 10
const SERVICE_2_TIMEOUT = 0

func shouldErr() bool {
	num := rand.Intn(11) // set error rate to be 30%
	fmt.Printf("num is %v ", num)
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
	ctx, cancel := context.WithTimeout(parentCtx, 1*time.Second)
	defer cancel()

	info, err := ua.Aggregate(ctx, 1)
	if err != nil {
		log.Fatal(err)
	}

	log.Println(info)
	log.Println("the end")
}

type UserAggregator struct {
}

func (ua *UserAggregator) Aggregate(ctx context.Context, id int) (Info, error) {
	var order = &Order{}
	os := &OrderService{}

	var profile = &Profile{}
	ps := &ProfileService{}

	eg, _ := errgroup.WithContext(ctx)

	eg.Go(func() error {
		var err error
		profile, err = ps.GetProfile(ctx)
		if err != nil {
			log.Println("err check in goroutine")
			return err
		}
		return nil
	})

	eg.Go(func() error {
		var err error
		order, err = os.GetOrders(ctx)
		if err != nil {
			return err
		}
		return nil
	})

	if err := eg.Wait(); err != nil {
		log.Fatal(err)
	}
	return Info{
		profile: *profile,
		order:   *order,
	}, nil
}

type Order struct {
	Orders int
}
type OrderService struct {
}

func (os *OrderService) GetOrders(ctx context.Context) (*Order, error) {
	log.Println("beginning of GetOrders")

	// if shouldErr() {
	// 	return nil, errors.New("mock orders error")
	// }
	//
	//
	select {
	case <-time.After(SERVICE_1_TIMEOUT * time.Second):
		fmt.Println("get orders checkpoint")
		return &Order{Orders: 5}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

type ProfileService struct {
}
type Profile struct {
	Name string
}

func (ps *ProfileService) GetProfile(ctx context.Context) (*Profile, error) {
	log.Println("beginning of GetProfile")
	// if true {
	// 	log.Println("error in get profile")
	// 	return nil, errors.New("mock profile error")
	// }
	// time.Sleep(6 * time.Second)
	select {
	case <-time.After(time.Duration(SERVICE_2_TIMEOUT) * time.Second):
		if true {
			return nil, errors.New("mock profile error")
		}
		return &Profile{Name: "Alice"}, nil
	case <-ctx.Done():
		return nil, ctx.Err()

	}
}

type Info struct {
	profile Profile
	order   Order
}
