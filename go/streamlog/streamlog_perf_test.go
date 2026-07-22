/*
Copyright 2026 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package streamlog

import (
	"runtime"
	"sync"
	"testing"
)

func benchmarkStreamLoggerSendParallel(b *testing.B, subscribe bool) {
	logger := New[*logMessage]("streamlogger-benchmark", 1)
	if subscribe {
		ch := logger.Subscribe("benchmark")
		b.Cleanup(func() {
			logger.Unsubscribe(ch)
		})
	}

	// Keep the send counter and, for the subscribed control, both delivery
	// counter paths out of the timed loop. The alternating messages keep the
	// generic argument live without allocating on each send.
	messages := [2]*logMessage{{val: "select 1"}, {val: "select 2"}}
	logger.Send(messages[0])
	if subscribe {
		logger.Send(messages[1])
	}

	b.ReportAllocs()
	b.SetParallelism(8)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for i := 0; pb.Next(); i++ {
			logger.Send(messages[i&1])
		}
	})
	b.StopTimer()
	runtime.KeepAlive(logger)
}

func TestStreamLoggerSubscriberLifecycle(t *testing.T) {
	logger := New[*logMessage]("subscriber-lifecycle", 1)
	first := &logMessage{val: "first"}
	second := &logMessage{val: "second"}

	// An empty logger must accept a send, and a completed subscription must
	// receive later messages synchronously through its buffered channel.
	logger.Send(first)
	ch := logger.Subscribe("test")
	logger.Send(first)
	if got := <-ch; got != first {
		t.Fatalf("received %p, want %p", got, first)
	}

	logger.Unsubscribe(ch)
	logger.Send(second)
	select {
	case got := <-ch:
		t.Fatalf("received %p after unsubscribe", got)
	default:
	}
}

func TestStreamLoggerConcurrentSendAndSubscription(t *testing.T) {
	logger := New[*logMessage]("concurrent-lifecycle", 1)
	messages := [2]*logMessage{{val: "select 1"}, {val: "select 2"}}
	start := make(chan struct{})
	stop := make(chan struct{})

	var senders sync.WaitGroup
	for range 4 {
		senders.Go(func() {
			<-start
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
					logger.Send(messages[i&1])
				}
			}
		})
	}

	close(start)
	for range 100 {
		ch := logger.Subscribe("test")
		logger.Unsubscribe(ch)
	}
	close(stop)
	senders.Wait()
}

func BenchmarkStreamLoggerSendNoSubscribersParallel(b *testing.B) {
	benchmarkStreamLoggerSendParallel(b, false)
}

// BenchmarkStreamLoggerSendWithSubscriberParallel is a contention-profile
// control: a subscriber requires Send to traverse the broadcaster mutex.
func BenchmarkStreamLoggerSendWithSubscriberParallel(b *testing.B) {
	benchmarkStreamLoggerSendParallel(b, true)
}
