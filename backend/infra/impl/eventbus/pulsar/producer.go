/*
 * Copyright 2025 coze-dev Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package pulsar

import (
	"context"
	"fmt"

	"github.com/apache/pulsar-client-go/pulsar"

	"github.com/coze-dev/coze-studio/backend/infra/contract/eventbus"
	"github.com/coze-dev/coze-studio/backend/pkg/lang/signal"
	"github.com/coze-dev/coze-studio/backend/pkg/safego"
)

type producerImpl struct {
	topic    string
	client   pulsar.Client
	producer pulsar.Producer
}

func NewProducer(serviceURL, topic, group string) (eventbus.Producer, error) {
	if serviceURL == "" {
		return nil, fmt.Errorf("service URL is empty")
	}

	if topic == "" {
		return nil, fmt.Errorf("topic is empty")
	}

	// Create Pulsar client
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: serviceURL,
	})
	if err != nil {
		return nil, fmt.Errorf("create pulsar client failed: %w", err)
	}

	// Create producer
	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic: topic,
		Name:  fmt.Sprintf("%s-producer", group),
	})
	if err != nil {
		client.Close()
		return nil, fmt.Errorf("create pulsar producer failed: %w", err)
	}

	impl := &producerImpl{
		topic:    topic,
		client:   client,
		producer: producer,
	}

	// Handle graceful shutdown
	safego.Go(context.Background(), func() {
		signal.WaitExit()
		impl.close()
	})

	return impl, nil
}

func (p *producerImpl) Send(ctx context.Context, body []byte, opts ...eventbus.SendOpt) error {
	return p.BatchSend(ctx, [][]byte{body}, opts...)
}

func (p *producerImpl) BatchSend(ctx context.Context, bodyArr [][]byte, opts ...eventbus.SendOpt) error {
	option := eventbus.SendOption{}
	for _, opt := range opts {
		opt(&option)
	}

	for _, body := range bodyArr {
		msg := &pulsar.ProducerMessage{
			Payload: body,
		}

		// Set partition key if sharding key is provided
		if option.ShardingKey != nil {
			msg.Key = *option.ShardingKey
		}

		// Send message synchronously
		_, err := p.producer.Send(ctx, msg)
		if err != nil {
			return fmt.Errorf("[pulsarProducer] send message failed: %w", err)
		}
	}

	return nil
}

func (p *producerImpl) close() {
	if p.producer != nil {
		p.producer.Close()
	}
	if p.client != nil {
		p.client.Close()
	}
}
