// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"gopkg.in/yaml.v3"

	"github.com/redpanda-data/connect/benchmarking/aws/plan"
)

const benchProjectTag = "redpanda-connect-bench"

var errTimeout = errors.New("timed out waiting for the shared bench stack")

// EC2API is the subset of *ec2.Client the lock wait uses.
type EC2API interface {
	DescribeInstances(context.Context, *ec2.DescribeInstancesInput, ...func(*ec2.Options)) (*ec2.DescribeInstancesOutput, error)
}

var _ EC2API = (*ec2.Client)(nil)

// dependencies are the external services, replaced in tests.
type dependencies struct {
	s3Objects func(ctx context.Context, bucket string) (plan.Objects, error)
	ec2       func(ctx context.Context) (EC2API, error)
	now       func() time.Time
}

var deps = dependencies{
	s3Objects: func(ctx context.Context, bucket string) (plan.Objects, error) {
		cfg, err := config.LoadDefaultConfig(ctx)
		if err != nil {
			return nil, fmt.Errorf("loading AWS config: %w", err)
		}
		return plan.NewS3Objects(s3.NewFromConfig(cfg), bucket), nil
	},
	ec2: func(ctx context.Context) (EC2API, error) {
		cfg, err := config.LoadDefaultConfig(ctx)
		if err != nil {
			return nil, fmt.Errorf("loading AWS config: %w", err)
		}
		return ec2.NewFromConfig(cfg), nil
	},
	now: time.Now,
}

// openObjects resolves --store: s3://<bucket> or dir:<path>.
func openObjects(ctx context.Context, loc string) (plan.Objects, error) {
	switch {
	case strings.HasPrefix(loc, "s3://") && len(loc) > len("s3://") && !strings.Contains(loc[len("s3://"):], "/"):
		return deps.s3Objects(ctx, strings.TrimPrefix(loc, "s3://"))
	case strings.HasPrefix(loc, "dir:") && len(loc) > len("dir:"):
		return plan.NewDirObjects(strings.TrimPrefix(loc, "dir:")), nil
	default:
		return nil, fmt.Errorf("%w: --store %q: want s3://<bucket> or dir:<path>", errUsage, loc)
	}
}

func newFlags(name string) *flag.FlagSet {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	return fs
}

func parse(fs *flag.FlagSet, args []string) error {
	if err := fs.Parse(args); err != nil {
		return fmt.Errorf("%w: %v", errUsage, err)
	}
	return nil
}

func runBuild(args []string, stdout io.Writer) error {
	fs := newFlags("build")
	var (
		cfgPath, mode, channel, thread, store string
		msgSize                               int
		target                                float64
	)
	fs.StringVar(&cfgPath, "config", "", "pipeline config YAML")
	fs.IntVar(&msgSize, "message-size", 0, "message size in bytes")
	fs.Float64Var(&target, "target-mb-s", 0, "target throughput in MB/s")
	fs.StringVar(&mode, "mode", "", "cdc or bulk (default: derived from the input)")
	fs.StringVar(&channel, "slack-channel", "", "Slack channel of the requesting thread")
	fs.StringVar(&thread, "slack-thread", "", "Slack thread timestamp")
	fs.StringVar(&store, "store", "", "s3://<bucket> or dir:<path>")
	if err := parse(fs, args); err != nil {
		return err
	}
	switch {
	case cfgPath == "":
		return fmt.Errorf("%w: --config is required", errUsage)
	case msgSize <= 0:
		return fmt.Errorf("%w: --message-size must be positive", errUsage)
	case target <= 0:
		return fmt.Errorf("%w: --target-mb-s must be positive", errUsage)
	case mode != "" && mode != "cdc" && mode != "bulk":
		return fmt.Errorf("%w: --mode must be cdc or bulk", errUsage)
	case store == "":
		return fmt.Errorf("%w: --store is required", errUsage)
	}
	ctx := context.Background()
	objs, err := openObjects(ctx, store)
	if err != nil {
		return err
	}
	raw, err := os.ReadFile(cfgPath)
	if err != nil {
		return fmt.Errorf("reading config: %w", err)
	}
	var cfg map[string]any
	if err := yaml.Unmarshal(raw, &cfg); err != nil {
		return fmt.Errorf("parsing config %s: %w", cfgPath, err)
	}
	p, err := plan.Build(cfg, plan.Workload{MessageSizeBytes: msgSize, TargetMBps: target, Mode: mode},
		plan.SlackRef{Channel: channel, ThreadTS: thread}, deps.now().UTC())
	if err != nil {
		return err
	}
	if err := plan.NewStore(objs).Put(ctx, p); err != nil {
		return err
	}
	return printPlan(stdout, p)
}

func runLock(args []string, stderr io.Writer) error {
	if len(args) == 0 {
		return fmt.Errorf("%w: lock <acquire|release> --store S --holder H", errUsage)
	}
	fs := newFlags("lock " + args[0])
	var (
		holder, store string
		ttl, wait     = plan.DefaultLockTTL, plan.DefaultLockTTL
		poll          = 2 * time.Minute
	)
	fs.StringVar(&holder, "holder", "", "lock holder id")
	fs.StringVar(&store, "store", "", "s3://<bucket> or dir:<path>")
	if args[0] == "acquire" {
		fs.DurationVar(&ttl, "ttl", ttl, "lock lifetime")
		fs.DurationVar(&wait, "wait", wait, "how long to wait for the stack")
		fs.DurationVar(&poll, "poll", poll, "poll interval")
	}
	if args[0] != "acquire" && args[0] != "release" {
		return fmt.Errorf("%w: unknown lock subcommand %q", errUsage, args[0])
	}
	if err := parse(fs, args[1:]); err != nil {
		return err
	}
	switch {
	case holder == "":
		return fmt.Errorf("%w: --holder is required", errUsage)
	case store == "":
		return fmt.Errorf("%w: --store is required", errUsage)
	case ttl <= 0 || wait < 0 || poll <= 0:
		return fmt.Errorf("%w: --ttl and --poll must be positive and --wait not negative", errUsage)
	}
	ctx := context.Background()
	objs, err := openObjects(ctx, store)
	if err != nil {
		return err
	}
	lock := plan.NewLock(objs)
	if args[0] == "release" {
		if err := lock.Release(ctx, holder); err != nil {
			return err
		}
		fmt.Fprintf(stderr, "released lock for %s (no-op if it was not the holder)\n", holder)
		return nil
	}
	api, err := deps.ec2(ctx)
	if err != nil {
		return err
	}
	return acquire(ctx, lock, api, holder, ttl, wait, poll, stderr)
}

// acquire waits until no bench instance is running, then takes the lock;
// the lock is only attempted once the stack is empty.
func acquire(ctx context.Context, lock plan.Lock, api EC2API, holder string, ttl, wait, poll time.Duration, log io.Writer) error {
	deadline := time.Now().Add(wait)
	var lastErr error
	for {
		n, err := runningBenchInstances(ctx, api)
		if err != nil {
			// Retry on the next poll; fail only if it persists to the deadline.
			fmt.Fprintf(log, "error checking bench instances (will retry): %v\n", err)
			lastErr = err
		} else if lastErr = nil; n == 0 {
			ok, err := lock.Acquire(ctx, holder, ttl)
			if err != nil {
				return err
			}
			if ok {
				fmt.Fprintf(log, "lock acquired by %s\n", holder)
				return nil
			}
			fmt.Fprintln(log, "stack lock held by another run; waiting")
		} else {
			fmt.Fprintf(log, "%d bench instance(s) still running; waiting\n", n)
		}
		if !time.Now().Add(poll).Before(deadline) {
			if lastErr != nil {
				return lastErr
			}
			return fmt.Errorf("%w: gave up after %s", errTimeout, wait)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(poll):
		}
	}
}

func runningBenchInstances(ctx context.Context, api EC2API) (int, error) {
	in := &ec2.DescribeInstancesInput{Filters: []ec2types.Filter{
		{Name: aws.String("tag:Project"), Values: []string{benchProjectTag}},
		{Name: aws.String("instance-state-name"), Values: []string{"pending", "running"}},
	}}
	n := 0
	for {
		out, err := api.DescribeInstances(ctx, in)
		if err != nil {
			return 0, fmt.Errorf("describing bench instances: %w", err)
		}
		for _, r := range out.Reservations {
			n += len(r.Instances)
		}
		if out.NextToken == nil {
			return n, nil
		}
		in.NextToken = out.NextToken
	}
}
