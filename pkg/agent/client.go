// Copyright 2024 LiveKit, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package agent 用于协调和管理 房间、参与者
// 任务（job）由客户端（client）分配给工作节点（worker），而代理（agent）则是一种特殊类型的工作节点，
// 可能会处理特定类型的任务。客户端负责与工作节点和代理进行通信，并管理任务的分配和执行过程。
package agent

import (
	"context"
	"sync"
	"time"

	"github.com/gammazero/workerpool"
	"google.golang.org/protobuf/types/known/emptypb"

	serverutils "github.com/livekit/livekit-server/pkg/utils"
	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/logger"
	"github.com/livekit/protocol/rpc"
	"github.com/livekit/protocol/utils"
	"github.com/livekit/psrpc"
)

const (
	// EnabledCacheTTL 用于缓存代理的启用(是否可用)状态。一旦缓存的时间超过了这个过期时间，
	// 代理客户端将会重新检查代理的启用状态，以确保缓存的状态信息是最新的
	EnabledCacheTTL = 1 * time.Minute
	// RoomAgentTopic 和 PublisherAgentTopic：用于区分房间任务和发布者任务的主题。
	RoomAgentTopic      = "room"
	PublisherAgentTopic = "publisher"
	// DefaultHandlerNamespace 默认的命名空间
	DefaultHandlerNamespace = ""

	// CheckEnabledTimeout 用于检查代理是否可用的超时时间
	CheckEnabledTimeout = 5 * time.Second
)

type Client interface {
	// LaunchJob starts a room or participant job on an agent.
	// it will launch a job once for each worker in each namespace
	// LaunchJob 在 agent 上启动一个 room 或 participant 任务
	// 它将在每个命名空间的每个 worker 上启动一个任务
	LaunchJob(ctx context.Context, desc *JobDescription)
	Stop() error
}

type JobDescription struct {
	JobType livekit.JobType
	Room    *livekit.Room
	// only set for participant jobs
	// 根据是否有参与者信息，来判断是房间任务还是参与者任务
	Participant *livekit.ParticipantInfo
}

// agentClient 代表了一个 agent 客户端, 用于处理 agent 的任务
type agentClient struct {
	client rpc.AgentInternalClient

	mu sync.RWMutex

	// cache response to avoid constantly checking with controllers
	// cache is invalidated with AgentRegistered updates
	roomNamespaces      *serverutils.IncrementalDispatcher[string]
	publisherNamespaces *serverutils.IncrementalDispatcher[string]
	enabledExpiresAt    time.Time

	workers *workerpool.WorkerPool

	invalidateSub psrpc.Subscription[*emptypb.Empty]
	subDone       chan struct{}
}

func NewAgentClient(bus psrpc.MessageBus) (Client, error) {
	client, err := rpc.NewAgentInternalClient(bus)
	if err != nil {
		return nil, err
	}

	c := &agentClient{
		client:  client,
		workers: workerpool.New(50),
		subDone: make(chan struct{}),
	}

	// 订阅工作节点注册的消息。订阅操作是针对消息总线（message bus）中的特定主题（topic）进行的，以便在该主题上接收到新消息时能够获取通知。
	// TODO: 还不太懂
	sub, err := c.client.SubscribeWorkerRegistered(context.Background(), DefaultHandlerNamespace)
	if err != nil {
		return nil, err
	}

	c.invalidateSub = sub

	go func() {
		// invalidate cache
		for range sub.Channel() {
			c.mu.Lock()
			c.roomNamespaces = nil
			c.publisherNamespaces = nil
			c.mu.Unlock()
		}

		c.subDone <- struct{}{}
	}()

	return c, nil
}

// LaunchJob 在 agent 上启动一个 room 或 participant 任务
func (c *agentClient) LaunchJob(ctx context.Context, desc *JobDescription) {
	roomNamespaces, publisherNamespaces, needsRefresh := c.getOrCreateDispatchers()

	if needsRefresh {
		go c.checkEnabled(ctx, roomNamespaces, publisherNamespaces)
	}

	target := roomNamespaces
	jobTypeTopic := RoomAgentTopic
	if desc.JobType == livekit.JobType_JT_PUBLISHER {
		target = publisherNamespaces
		jobTypeTopic = PublisherAgentTopic
	}

	target.ForEach(func(ns string) {
		c.workers.Submit(func() {
			_, err := c.client.JobRequest(ctx, ns, jobTypeTopic, &livekit.Job{
				Id:          utils.NewGuid(utils.AgentJobPrefix),
				Type:        desc.JobType,
				Room:        desc.Room,
				Participant: desc.Participant,
				Namespace:   ns,
			})
			if err != nil {
				logger.Errorw("failed to send job request", err, "namespace", ns, "jobType", jobTypeTopic)
			}
		})
	})
}

// getOrCreateDispatchers 获取或创建 dispatchers(调度器)
func (c *agentClient) getOrCreateDispatchers() (*serverutils.IncrementalDispatcher[string], *serverutils.IncrementalDispatcher[string], bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if time.Since(c.enabledExpiresAt) > EnabledCacheTTL || c.roomNamespaces == nil || c.publisherNamespaces == nil {
		c.roomNamespaces = serverutils.NewIncrementalDispatcher[string]()
		c.publisherNamespaces = serverutils.NewIncrementalDispatcher[string]()
		return c.roomNamespaces, c.publisherNamespaces, true
	}
	return c.roomNamespaces, c.publisherNamespaces, false
}

// checkEnabled 这个方法的主要作用是根据代理的启用状态更新 room 命名空间和 publisher 命名空间，以便后续的任务调度和处理能够基于最新的命名空间信息进行。
func (c *agentClient) checkEnabled(ctx context.Context, roomNamespaces, publisherNamespaces *serverutils.IncrementalDispatcher[string]) {
	defer roomNamespaces.Done()
	defer publisherNamespaces.Done()
	resChan, err := c.client.CheckEnabled(ctx, &rpc.CheckEnabledRequest{}, psrpc.WithRequestTimeout(CheckEnabledTimeout))
	if err != nil {
		logger.Errorw("failed to check enabled", err)
		return
	}

	// 两个 map 作用，避免重复添加相同的命名空间
	roomNSMap := make(map[string]bool)
	publisherNSMap := make(map[string]bool)

	for r := range resChan {
		if r.Result.GetRoomEnabled() {
			for _, ns := range r.Result.GetNamespaces() {
				if _, ok := roomNSMap[ns]; !ok {
					roomNamespaces.Add(ns)
					roomNSMap[ns] = true
				}
			}
		}
		if r.Result.GetPublisherEnabled() {
			for _, ns := range r.Result.GetNamespaces() {
				if _, ok := publisherNSMap[ns]; !ok {
					publisherNamespaces.Add(ns)
					publisherNSMap[ns] = true
				}
			}
		}
	}
}

func (c *agentClient) Stop() error {
	_ = c.invalidateSub.Close()
	<-c.subDone
	return nil
}
