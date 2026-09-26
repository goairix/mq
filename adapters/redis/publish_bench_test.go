package redisadapter

import (
	"context"
	"net"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	mq "github.com/goairix/mq/v2"
	"github.com/redis/go-redis/v9"
)

// BenchmarkConfirmedBatch compares the adapter against a direct go-redis
// pipeline using the same client, stream, payload and XADD acknowledgement.
func BenchmarkConfirmedBatch(b *testing.B) {
	addr := os.Getenv("MQ_TEST_REDIS_ADDR")
	clusterAddresses := os.Getenv("MQ_TEST_REDIS_CLUSTER_ADDR")
	if addr == "" && clusterAddresses == "" {
		b.Skip("set MQ_TEST_REDIS_ADDR or MQ_TEST_REDIS_CLUSTER_ADDR")
	}
	var client redis.UniversalClient
	if clusterAddresses != "" {
		client = redis.NewClusterClient(&redis.ClusterOptions{
			Addrs: strings.Split(clusterAddresses, ","),
			Dialer: func(ctx context.Context, network, address string) (net.Conn, error) {
				_, port, err := net.SplitHostPort(address)
				if err != nil {
					return nil, err
				}
				return (&net.Dialer{}).DialContext(ctx, network, net.JoinHostPort("127.0.0.1", port))
			},
		})
	} else {
		client = redis.NewClient(&redis.Options{Addr: addr})
	}
	defer client.Close()
	adapter, err := New(client, Options{Prefix: "mq:v2:bench:" + time.Now().Format("150405.000000000")})
	if err != nil {
		b.Fatal(err)
	}
	defer adapter.Close(context.Background())
	const batchSize = 256
	const payloadBytes = 1024
	messages := make([]mq.Message, batchSize)
	for index := range messages {
		messages[index], err = mq.NewMessage("confirmed", make([]byte, payloadBytes))
		if err != nil {
			b.Fatal(err)
		}
	}
	adapterPublish := func(ctx context.Context) error {
		for _, result := range adapter.PublishBatch(ctx, messages) {
			if result.State != mq.PublishAccepted {
				return result.Err
			}
		}
		return nil
	}
	directPublish := func(ctx context.Context) error {
		pipeline := client.Pipeline()
		commands := make([]*redis.StringCmd, len(messages))
		for index, message := range messages {
			commands[index] = pipeline.XAdd(ctx, &redis.XAddArgs{Stream: adapter.streamKey(message.Topic), Values: encode(message)})
		}
		_, _ = pipeline.Exec(ctx)
		for _, command := range commands {
			if err := command.Err(); err != nil {
				return err
			}
		}
		return nil
	}
	measure := func(b *testing.B, publish func(context.Context) error) {
		b.SetBytes(batchSize * payloadBytes)
		b.ReportAllocs()
		latencies := make([]time.Duration, 0, b.N)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		b.ResetTimer()
		for range b.N {
			start := time.Now()
			if err := publish(ctx); err != nil {
				b.Fatal(err)
			}
			latencies = append(latencies, time.Since(start))
		}
		b.StopTimer()
		sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
		if len(latencies) > 0 {
			b.ReportMetric(float64(latencies[(len(latencies)*95-1)/100].Microseconds())/1000, "p95_ms")
		}
	}
	b.Run("adapter", func(b *testing.B) {
		measure(b, adapterPublish)
	})
	b.Run("direct_go_redis", func(b *testing.B) {
		measure(b, directPublish)
	})
	b.Run("paired", func(b *testing.B) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		var adapterElapsed, directElapsed time.Duration
		b.ResetTimer()
		for index := range b.N {
			first, second := adapterPublish, directPublish
			if index%2 == 1 {
				first, second = directPublish, adapterPublish
			}
			start := time.Now()
			if err := first(ctx); err != nil {
				b.Fatal(err)
			}
			firstElapsed := time.Since(start)
			start = time.Now()
			if err := second(ctx); err != nil {
				b.Fatal(err)
			}
			secondElapsed := time.Since(start)
			if index%2 == 0 {
				adapterElapsed += firstElapsed
				directElapsed += secondElapsed
			} else {
				directElapsed += firstElapsed
				adapterElapsed += secondElapsed
			}
		}
		b.StopTimer()
		if adapterElapsed > 0 && directElapsed > 0 {
			b.ReportMetric(100*float64(directElapsed)/float64(adapterElapsed), "adapter_pct")
			b.ReportMetric(float64(b.N*batchSize*payloadBytes)/adapterElapsed.Seconds()/1e6, "adapter_MB/s")
			b.ReportMetric(float64(b.N*batchSize*payloadBytes)/directElapsed.Seconds()/1e6, "direct_MB/s")
		}
	})
}
