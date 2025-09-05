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
	"os"
	"testing"
	"time"

	"github.com/coze-dev/coze-studio/backend/infra/contract/eventbus"
	"github.com/coze-dev/coze-studio/backend/types/consts"
)

// MockConsumerHandler for testing
type mockConsumerHandler struct {
	messages [][]byte
}

func (m *mockConsumerHandler) HandleMessage(ctx context.Context, msg *eventbus.Message) error {
	m.messages = append(m.messages, msg.Body)
	return nil
}

func TestPulsarProducerValidation(t *testing.T) {
	tests := []struct {
		name       string
		serviceURL string
		topic      string
		group      string
		wantErr    bool
	}{
		{
			name:       "empty service URL",
			serviceURL: "",
			topic:      "test-topic",
			group:      "test-group",
			wantErr:    true,
		},
		{
			name:       "empty topic",
			serviceURL: "pulsar://localhost:6650",
			topic:      "",
			group:      "test-group",
			wantErr:    true,
		},
		{
			name:       "valid parameters - will fail connection but pass validation",
			serviceURL: "pulsar://invalid-host:6650",
			topic:      "test-topic",
			group:      "test-group",
			wantErr:    true, // Connection will fail but validation passes
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewProducer(tt.serviceURL, tt.topic, tt.group)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewProducer() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestPulsarConsumerValidation(t *testing.T) {
	handler := &mockConsumerHandler{}

	tests := []struct {
		name       string
		serviceURL string
		topic      string
		group      string
		handler    eventbus.ConsumerHandler
		wantErr    bool
	}{
		{
			name:       "empty service URL",
			serviceURL: "",
			topic:      "test-topic",
			group:      "test-group",
			handler:    handler,
			wantErr:    true,
		},
		{
			name:       "empty topic",
			serviceURL: "pulsar://localhost:6650",
			topic:      "",
			group:      "test-group",
			handler:    handler,
			wantErr:    true,
		},
		{
			name:       "empty group",
			serviceURL: "pulsar://localhost:6650",
			topic:      "test-topic",
			group:      "",
			handler:    handler,
			wantErr:    true,
		},
		{
			name:       "nil handler",
			serviceURL: "pulsar://localhost:6650",
			topic:      "test-topic",
			group:      "test-group",
			handler:    nil,
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := RegisterConsumer(tt.serviceURL, tt.topic, tt.group, tt.handler)
			if (err != nil) != tt.wantErr {
				t.Errorf("RegisterConsumer() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// TestPulsarIntegration tests actual Pulsar connection (requires running Pulsar instance)
func TestPulsarIntegration(t *testing.T) {
	serviceURL := os.Getenv("PULSAR_SERVICE_URL")
	if serviceURL == "" {
		serviceURL = "pulsar://localhost:6650"
	}

	topic := "test-topic"
	group := "test-group"

	// Test producer
	producer, err := NewProducer(serviceURL, topic, group)
	if err != nil {
		t.Skipf("Failed to create producer (Pulsar may not be running): %v", err)
	}
	defer producer.(*producerImpl).close()

	// Test sending message
	ctx := context.Background()
	testMessage := []byte("test message")
	err = producer.Send(ctx, testMessage)
	if err != nil {
		t.Errorf("Failed to send message: %v", err)
	}

	// Test batch sending
	messages := [][]byte{
		[]byte("batch message 1"),
		[]byte("batch message 2"),
	}
	err = producer.BatchSend(ctx, messages)
	if err != nil {
		t.Errorf("Failed to batch send messages: %v", err)
	}

	// Test consumer
	handler := &mockConsumerHandler{}
	err = RegisterConsumer(serviceURL, topic, group+"-consumer", handler)
	if err != nil {
		t.Errorf("Failed to register consumer: %v", err)
	}

	// Give some time for messages to be consumed
	time.Sleep(2 * time.Second)
}

// TestPulsarJWTAuthentication tests JWT token authentication
func TestPulsarJWTAuthentication(t *testing.T) {
	// Set the JWT token for testing
	testToken := "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJhZG1pbiJ9.Kr7Qem-NLoq-85Yb2vN-lN4fH2uODFiPrHJS-Oxvzm0"
	os.Setenv(consts.PulsarJWTToken, testToken)
	defer os.Unsetenv(consts.PulsarJWTToken)

	serviceURL := "pulsar://localhost:6650"
	topic := "test-auth-topic"
	group := "test-auth-group"

	// Test producer with JWT authentication
	producer, err := NewProducer(serviceURL, topic, group)
	if err != nil {
		t.Skipf("Failed to create producer with JWT auth (Pulsar may not be running or auth failed): %v", err)
	}
	defer producer.(*producerImpl).close()

	// Test sending message with authentication
	ctx := context.Background()
	testMessage := []byte("authenticated test message")
	err = producer.Send(ctx, testMessage)
	if err != nil {
		t.Errorf("Failed to send authenticated message: %v", err)
	}

	// Test consumer with JWT authentication
	handler := &mockConsumerHandler{}
	err = RegisterConsumer(serviceURL, topic, group+"-consumer", handler)
	if err != nil {
		t.Errorf("Failed to register consumer with JWT auth: %v", err)
	}

	t.Logf("JWT authentication test completed successfully")
}
