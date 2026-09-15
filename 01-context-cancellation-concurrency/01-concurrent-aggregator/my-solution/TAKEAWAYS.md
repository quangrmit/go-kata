# What did I learn?


I learnt how to use the `errgroup` package
    I have learnt that error groups still wait when we return error in one of the operations, unless you check for `ctx.Done()` and return `ctx.Err()`, and this is how the global timeout is enforced in the code




I've also learnt how to mock latency when we also need to check for `ctx.Done()` in my service
    for this I use `<-time.After()`

And that I don't need to use channels when using `errgroup` for checking global timeouts, because that is already handled, if I check the `ctx.Done()` in downstream services
