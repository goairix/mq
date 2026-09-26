package kafkaadapter

import (
	"context"
	"errors"
	"fmt"
	"testing"

	mq "github.com/goairix/mq/v2"
	"github.com/goairix/mq/v2/contracttest"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestPortableContract(t *testing.T) {
	brokers := testBrokers(t)
	contracttest.Run(t, func(t *testing.T) contracttest.Transport {
		t.Helper()
		a, err := New(brokers, Options{})
		if err != nil {
			t.Fatal(err)
		}
		return contracttest.Transport{
			Publisher: a, Subscriber: a, BatchSubscriber: a,
			Prepare: func(ctx context.Context, sub mq.Subscription) error {
				if err := a.Prepare(ctx, sub); err != nil {
					return err
				}
				return createContractTopics(ctx, a.producer, sub.Topic, sub.Topic+a.options.DLQSuffix)
			},
			Outstanding: func(ctx context.Context, sub mq.Subscription) (int64, error) {
				return kafkaOutstanding(ctx, a.producer, sub)
			},
		}
	})
}

func createContractTopics(ctx context.Context, client *kgo.Client, names ...string) error {
	request := kmsg.NewCreateTopicsRequest()
	request.Topics = make([]kmsg.CreateTopicsRequestTopic, 0, len(names))
	for _, name := range names {
		topic := kmsg.NewCreateTopicsRequestTopic()
		topic.Topic, topic.NumPartitions, topic.ReplicationFactor = name, 1, 1
		request.Topics = append(request.Topics, topic)
	}
	response, err := request.RequestWith(ctx, client)
	if err != nil {
		return err
	}
	for _, topic := range response.Topics {
		if err := kerr.ErrorForCode(topic.ErrorCode); err != nil && !errors.Is(err, kerr.TopicAlreadyExists) {
			return fmt.Errorf("create Kafka test topic %s: %w", topic.Topic, err)
		}
	}
	return nil
}

// kafkaOutstanding is a test-only group lag probe for the portable contract.
// Contract topics are unique, so an absent group commit starts at offset zero.
func kafkaOutstanding(ctx context.Context, client *kgo.Client, sub mq.Subscription) (int64, error) {
	metadata, err := (&kmsg.MetadataRequest{Topics: []kmsg.MetadataRequestTopic{{Topic: &sub.Topic}}}).RequestWith(ctx, client)
	if err != nil {
		return 0, err
	}
	if len(metadata.Topics) != 1 {
		return 0, fmt.Errorf("metadata returned %d topics", len(metadata.Topics))
	}
	topic := metadata.Topics[0]
	if err := kerr.ErrorForCode(topic.ErrorCode); err != nil {
		return 0, err
	}
	partitions := make([]int32, 0, len(topic.Partitions))
	latest := kmsg.NewListOffsetsRequest()
	latestTopic := kmsg.NewListOffsetsRequestTopic()
	latestTopic.Topic = sub.Topic
	for _, partition := range topic.Partitions {
		if err := kerr.ErrorForCode(partition.ErrorCode); err != nil {
			return 0, err
		}
		partitions = append(partitions, partition.Partition)
		p := kmsg.NewListOffsetsRequestTopicPartition()
		p.Partition, p.Timestamp = partition.Partition, -1
		latestTopic.Partitions = append(latestTopic.Partitions, p)
	}
	latest.Topics = []kmsg.ListOffsetsRequestTopic{latestTopic}
	latestResponse, err := latest.RequestWith(ctx, client)
	if err != nil {
		return 0, err
	}
	committed := kmsg.NewOffsetFetchRequest()
	committed.Group = sub.Name
	committed.Topics = []kmsg.OffsetFetchRequestTopic{{Topic: sub.Topic, Partitions: partitions}}
	group := kmsg.NewOffsetFetchRequestGroup()
	group.Group = sub.Name
	group.Topics = []kmsg.OffsetFetchRequestGroupTopic{{Topic: sub.Topic, Partitions: partitions}}
	committed.Groups = []kmsg.OffsetFetchRequestGroup{group}
	commitResponse, err := committed.RequestWith(ctx, client)
	if err != nil {
		return 0, err
	}
	offsets := make(map[int32]int64, len(partitions))
	if len(commitResponse.Groups) > 0 {
		for _, group := range commitResponse.Groups {
			if err := kerr.ErrorForCode(group.ErrorCode); err != nil {
				return 0, err
			}
			for _, topic := range group.Topics {
				for _, partition := range topic.Partitions {
					if err := kerr.ErrorForCode(partition.ErrorCode); err != nil {
						return 0, err
					}
					offsets[partition.Partition] = partition.Offset
				}
			}
		}
	} else {
		if err := kerr.ErrorForCode(commitResponse.ErrorCode); err != nil {
			return 0, err
		}
		for _, topic := range commitResponse.Topics {
			for _, partition := range topic.Partitions {
				if err := kerr.ErrorForCode(partition.ErrorCode); err != nil {
					return 0, err
				}
				offsets[partition.Partition] = partition.Offset
			}
		}
	}
	var lag int64
	for _, topic := range latestResponse.Topics {
		for _, partition := range topic.Partitions {
			if err := kerr.ErrorForCode(partition.ErrorCode); err != nil {
				return 0, err
			}
			start := offsets[partition.Partition]
			if start < 0 {
				start = 0
			}
			lag += max(0, partition.Offset-start)
		}
	}
	return lag, nil
}
