// Copyright 2024 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package discord

import (
	"sync"
	"sync/atomic"

	"github.com/bwmarrin/discordgo"
)

type refCountedSession struct {
	count int64
	sess  *discordgo.Session
}

type refCountedSessions struct {
	mut      sync.Mutex
	sessions map[string]*refCountedSession
}

func (r *refCountedSessions) done(botToken string) {
	r.mut.Lock()
	defer r.mut.Unlock()

	c, exists := r.sessions[botToken]
	if !exists {
		return
	}

	count := atomic.AddInt64(&c.count, -1)
	if count > 0 {
		return
	}

	_ = c.sess.Close()
	delete(r.sessions, botToken)
}

func (r *refCountedSessions) Get(botToken, benthosVersion string) (sess *discordgo.Session, done func(), err error) {
	done = func() {
		r.done(botToken)
	}

	r.mut.Lock()
	defer r.mut.Unlock()

	c, exists := globalSessions.sessions[botToken]
	if exists {
		atomic.AddInt64(&c.count, 1)
		sess = c.sess
		return
	}

	if sess, err = discordgo.New("Bot " + botToken); err != nil {
		return
	}
	sess.UserAgent = "Benthos " + benthosVersion
	sess.Identify.Intents |= discordgo.IntentMessageContent
	if err = sess.Open(); err != nil {
		return
	}

	globalSessions.sessions[botToken] = &refCountedSession{
		count: 1,
		sess:  sess,
	}
	return
}

var globalSessions = &refCountedSessions{
	sessions: map[string]*refCountedSession{},
}

func getGlobalSession(botToken, benthosVersion string) (*discordgo.Session, func(), error) {
	return globalSessions.Get(botToken, benthosVersion)
}
