package main

import (
	"context"
	"errors"
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

	*/
	ua := &UserAggregator{}
	parentCtx := context.Background()
	ctx, cancel := context.WithTimeout(parentCtx, 5*time.Second)
	defer cancel()

	err := ua.Aggregate(ctx, 1)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("the end")
}

type UserAggregator struct {
}

func (ua *UserAggregator) Aggregate(context context.Context, id int) error {
	co := make(chan Order)
	os := &OrderService{}
	eg := new(errgroup.Group)
	eg.Go(func() error {

		err := os.GetOrders(co)
		if err != nil {
			return err
		}
		return nil
	})

	select {
	case <-co:
		log.Println("received co")
	case <-context.Done():
		log.Fatal("context deadline exceeded")
		return errors.New("context deadline exceeded")
	}
	if err := eg.Wait(); err != nil {
		log.Fatal(err)
	}
	return nil
}

type Order struct {
	Orders int
}
type OrderService struct {
}

func (os *OrderService) GetOrders(oc chan Order) error {
	log.Println("beginning of GetOrders")

	if shouldErr() {
		return errors.New("fake error")
	}

	time.Sleep(6 * time.Second)

	oc <- Order{Orders: 5}
	return nil
}

type ProfileService struct {
}
type Profile struct {
	Name string
}

func (ps *ProfileService) GetProfile() Profile {
	return Profile{Name: "Alice"}
}
