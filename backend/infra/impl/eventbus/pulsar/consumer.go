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
	"github.com/coze-dev/coze-studio/backend/pkg/logs"
	"github.com/coze-dev/coze-studio/backend/pkg/safego"
)

func RegisterConsumer(serviceURL, topic, group string, consumerHandler eventbus.ConsumerHandler, opts ...eventbus.ConsumerOpt) error {
	if serviceURL == "" {
		return fmt.Errorf("service URL is empty")
	}
	if topic == "" {
		return fmt.Errorf("topic is empty")
	}
	if group == "" {
		return fmt.Errorf("group is empty")
	}
	if consumerHandler == nil {
		return fmt.Errorf("consumer handler is nil")
	}

	// Parse consumer options
	option := &eventbus.ConsumerOption{}
	for _, opt := range opts {
		opt(option)
	}

	// Create Pulsar client
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: serviceURL,
	})
	if err != nil {
		return fmt.Errorf("create pulsar client failed: %w", err)
	}

	// Configure consumer options
	consumerOptions := pulsar.ConsumerOptions{
		Topic:            topic,
		SubscriptionName: group,
		Type:             pulsar.Shared, // Default to shared subscription
	}

	// Handle orderly consumption
	if option.Orderly != nil && *option.Orderly {
		consumerOptions.Type = pulsar.Exclusive
	}

	// Create consumer
	consumer, err := client.Subscribe(consumerOptions)
	if err != nil {
		client.Close()
		return fmt.Errorf("create pulsar consumer failed: %w", err)
	}

	// Start consuming messages in a goroutine
	ctx := context.Background()
	safego.Go(ctx, func() {
		defer func() {
			consumer.Close()
			client.Close()
		}()

		for {
			select {
			case <-ctx.Done():
				return
			default:
				// Receive message
				msg, err := consumer.Receive(ctx)
				if err != nil {
					logs.Errorf("receive pulsar message error: %v", err)
					continue
				}

				// Convert to eventbus message
				eventMsg := &eventbus.Message{
					Topic: topic,
					Group: group,
					Body:  msg.Payload(),
				}

				// Handle message
				if err := consumerHandler.HandleMessage(ctx, eventMsg); err != nil {
					logs.Errorf("handle pulsar message failed, topic: %s, group: %s, err: %v", topic, group, err)
					// Negative acknowledge on error
					consumer.Nack(msg)
					continue
				}

				// Acknowledge message on success
				consumer.Ack(msg)
			}
		}
	})

	// Handle graceful shutdown
	safego.Go(ctx, func() {
		signal.WaitExit()
		consumer.Close()
		client.Close()
	})

	return nil
}
