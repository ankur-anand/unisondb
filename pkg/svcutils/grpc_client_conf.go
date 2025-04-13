package svcutils

// https://github.com/grpc/proposal/blob/master/A6-client-retries.md

type RetryPolicy struct {
	MaxAttempts          int      `json:"maxAttempts"`
	InitialBackoff       string   `json:"initialBackoff"`
	MaxBackoff           string   `json:"maxBackoff"`
	BackoffMultiplier    float64  `json:"backoffMultiplier"`
	RetryableStatusCodes []string `json:"retryableStatusCodes"`
}

type MethodName struct {
	Service string `json:"service"`
	Method  string `json:"method,omitempty"`
}

type MethodConfig struct {
	Name            []MethodName `json:"name"`
	Timeout         string       `json:"timeout,omitempty"`
	RetryPolicy     *RetryPolicy `json:"retryPolicy,omitempty"`
	MaxReqMsgBytes  int          `json:"maxRequestMessageBytes,omitempty"`
	MaxRespMsgBytes int          `json:"maxResponseMessageBytes,omitempty"`
	WaitForReady    bool         `json:"waitForReady,omitempty"`
}

type PickFirstConfig struct {
	ShuffleAddressList bool `json:"shuffle_address_list"`
}

type LoadBalancingConfig struct {
	PickFirst *PickFirstConfig `json:"pick_first,omitempty"`
}

type ServiceConfig struct {
	LoadBalancingConfig []LoadBalancingConfig `json:"loadBalancingConfig"`
	MethodConfig        []MethodConfig        `json:"methodConfig"`
}
