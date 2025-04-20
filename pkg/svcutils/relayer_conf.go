package svcutils

import (
	"encoding/json"
	"log"
)

// BuildDefaultRelayerServiceConfigJSON build Config JSON for grpc client for the WalStreaming Service.
func BuildDefaultRelayerServiceConfigJSON() string {
	cfg := ServiceConfig{
		LoadBalancingConfig: []LoadBalancingConfig{
			{
				PickFirst: &PickFirstConfig{ShuffleAddressList: true},
			},
		},
		MethodConfig: []MethodConfig{
			{
				Name: []MethodName{
					{Service: "unisondb.streamer.v1.WalStreamerService", Method: "StreamWalRecords"},
					{Service: "unisondb.streamer.v1.WalStreamerService", Method: "GetLatestOffset"},
				},
				RetryPolicy: &RetryPolicy{
					MaxAttempts:          5,
					InitialBackoff:       "0.2s",
					MaxBackoff:           "5s",
					BackoffMultiplier:    1.5,
					RetryableStatusCodes: []string{"UNAVAILABLE"},
				},
				WaitForReady: false,
			},
		},
	}

	data, err := json.Marshal(cfg)
	if err != nil {
		log.Fatalf("failed to marshal service config: %v", err)
	}

	return string(data)
}
