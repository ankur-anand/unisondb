package svcutils_test

import (
	"encoding/json"
	"testing"

	"github.com/ankur-anand/unisondb/pkg/svcutils"
)

func TestBuildDefaultServiceConfig(t *testing.T) {
	jsonStr := svcutils.BuildDefaultRelayerServiceConfigJSON()
	// {
	//  "loadBalancingConfig": [
	//    {
	//      "pick_first": {
	//        "shuffle_address_list": true
	//      }
	//    }
	//  ],
	//  "methodConfig": [
	//    {
	//      "name": [
	//        {
	//          "service": "unisondb.streamer.v1.WalStreamerService",
	//          "method": "StreamWalRecords"
	//        },
	//        {
	//          "service": "unisondb.streamer.v1.WalStreamerService",
	//          "method": "GetLatestOffset"
	//        }
	//      ],
	//      "timeout": "10s",
	//      "retryPolicy": {
	//        "maxAttempts": 5,
	//        "initialBackoff": "0.2s",
	//        "maxBackoff": "1s",
	//        "backoffMultiplier": 1.5,
	//        "retryableStatusCodes": [
	//          "UNAVAILABLE"
	//        ]
	//      }
	//    }
	//  ]
	//}

	var parsed map[string]interface{}
	if err := json.Unmarshal([]byte(jsonStr), &parsed); err != nil {
		t.Fatalf("invalid JSON returned: %v", err)
	}

	lbCfgs, ok := parsed["loadBalancingConfig"].([]interface{})
	if !ok || len(lbCfgs) == 0 {
		t.Fatalf("missing or invalid loadBalancingConfig")
	}
	if _, ok := lbCfgs[0].(map[string]interface{})["pick_first"]; !ok {
		t.Errorf("pick_first not found in loadBalancingConfig")
	}

	methods := parsed["methodConfig"].([]interface{})
	if len(methods) != 1 {
		t.Errorf("expected one methodConfig, got %d", len(methods))
	}

	methodCfg := methods[0].(map[string]interface{})
	retryPolicy := methodCfg["retryPolicy"].(map[string]interface{})
	if retryPolicy["maxAttempts"] != float64(5) {
		t.Errorf("unexpected maxAttempts: %v", retryPolicy["maxAttempts"])
	}
}
