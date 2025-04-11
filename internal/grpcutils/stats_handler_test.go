package grpcutils

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/common/expfmt"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"
)

type mockAddr struct {
	addr string
}

func (m mockAddr) String() string  { return m.addr }
func (m mockAddr) Network() string { return "tcp" }

func TestGRPCStatsHandler(t *testing.T) {
	methodInfo := map[string]string{
		"/service/method":  bidiStream,
		"/cservice/method": clientStream,
		"/uservice/method": unary,
		"/sservice/method": serverStream,
	}
	handler := NewGRPCStatsHandler(methodInfo)

	t.Run("test_conn_tag", func(t *testing.T) {
		tests := []struct {
			name      string
			info      *stats.ConnTagInfo
			wantAddr  string
			checkTime bool
		}{
			{
				name: "valid remote address",
				info: &stats.ConnTagInfo{
					RemoteAddr: mockAddr{
						addr: "127.0.0.1:8080",
					},
				},
				wantAddr:  "127.0.0.1:8080",
				checkTime: true,
			},
			{
				name:      "nil remote address",
				info:      &stats.ConnTagInfo{},
				wantAddr:  "unknown",
				checkTime: true,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				ctx := context.Background()
				taggedCtx := handler.TagConn(ctx, tt.info)

				addr, ok := taggedCtx.Value(ctxKeyRemoteAddr).(string)
				assert.True(t, ok)
				assert.Equal(t, tt.wantAddr, addr)

				if tt.checkTime {
					startTime, ok := taggedCtx.Value(ctxKeyConnStartTime).(time.Time)
					assert.True(t, ok)
					assert.False(t, startTime.IsZero())
				}
			})
		}
	})

	t.Run("test_tag_rpc", func(t *testing.T) {
		tests := []struct {
			name        string
			fullMethod  string
			wantService string
			wantMethod  string
			checkRPCID  bool
		}{
			{
				name:        "valid method name",
				fullMethod:  "/service/method",
				wantService: "service",
				wantMethod:  "method",
				checkRPCID:  true,
			},
			{
				name:        "invalid method name",
				fullMethod:  "invalid",
				wantService: "unknown",
				wantMethod:  "unknown",
				checkRPCID:  true,
			},
			{
				name:        "empty method name",
				fullMethod:  "",
				wantService: "unknown",
				wantMethod:  "unknown",
				checkRPCID:  true,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				ctx := context.Background()
				taggedCtx := handler.TagRPC(ctx, &stats.RPCTagInfo{FullMethodName: tt.fullMethod})

				service, ok := taggedCtx.Value(ctxKeyRPCService).(string)
				assert.True(t, ok)
				assert.Equal(t, tt.wantService, service)

				method, ok := taggedCtx.Value(ctxKeyRPCMethod).(string)
				assert.True(t, ok)
				assert.Equal(t, tt.wantMethod, method)

				if tt.checkRPCID {
					rpcID, ok := taggedCtx.Value(ctxKeyRPCID).(string)
					assert.True(t, ok)
					_, err := uuid.Parse(rpcID)
					assert.NoError(t, err)
				}
			})
		}
	})

	t.Run("test_handle_rpc", func(t *testing.T) {

		loop := 0
		errors := []error{
			nil,
			fmt.Errorf("some error"),
			status.Error(codes.ResourceExhausted, "some error"),
			status.Error(codes.Unavailable, "some error"),
		}
		for method := range methodInfo {
			loop++
			service, m := parseFullMethodName(method)
			ctx := context.Background()
			ctx = context.WithValue(ctx, ctxKeyRPCService, service)
			ctx = context.WithValue(ctx, ctxKeyRPCMethod, m)
			ctx = context.WithValue(ctx, ctxKeyRPCID, uuid.New().String())
			begin := &stats.Begin{
				BeginTime: time.Now().Add(-10 * time.Minute),
			}
			handler.HandleRPC(ctx, begin)

			assert.Equal(t, loop, testutil.CollectAndCount(handler.rpcHandled))
			assert.Equal(t, loop, testutil.CollectAndCount(handler.rpcInFlight))

			inPayload := &stats.InPayload{
				WireLength: 100,
			}
			handler.HandleRPC(ctx, inPayload)
			assert.Equal(t, loop, testutil.CollectAndCount(handler.recvMessage))

			outPayload := &stats.OutPayload{
				WireLength: 200,
			}
			handler.HandleRPC(ctx, outPayload)
			assert.Equal(t, loop, testutil.CollectAndCount(handler.sentMessage))
			end := &stats.End{
				BeginTime: begin.BeginTime,
				EndTime:   time.Now(),
				Error:     errors[loop-1],
			}
			handler.HandleRPC(ctx, end)
			assert.Equal(t, loop, testutil.CollectAndCount(handler.rpcDuration))
		}
	})

	t.Run("test_handle_rpc_error", func(t *testing.T) {
		for i := 0; i < 3; i++ {
			rpcID := uuid.New().String()
			info := streamInfo{
				service: fmt.Sprintf("service%d", i),
				method:  fmt.Sprintf("method%d", i),
				// when i ==0 this will be of short interval only.
				start:      time.Now().Add(-time.Duration(i) * time.Hour),
				streamType: serverStream,
			}
			handler.activeStreamsMap.Store(rpcID, info)
		}

		handler.UpdateStreamAgeBuckets()
		assert.Equal(t, 3, testutil.CollectAndCount(handler.streamAgeBucket))
	})

	t.Run("test_conn_handle", func(t *testing.T) {
		ctx := context.Background()
		ctx = context.WithValue(ctx, ctxKeyRemoteAddr, "127.0.0.1:90")
		ctx = context.WithValue(ctx, ctxKeyConnStartTime, time.Now().Add(-10*time.Minute))

		cb := stats.ConnBegin{Client: false}
		handler.HandleConn(ctx, &cb)

		assert.Equal(t, float64(1), testutil.ToFloat64(handler.activeConn))
		ce := stats.ConnEnd{Client: false}
		handler.HandleConn(ctx, &ce)
	})

	t.Run("prom_gatherer", func(t *testing.T) {
		gather, err := prometheus.DefaultGatherer.Gather()
		assert.NoError(t, err)
		var gotFormatted bytes.Buffer
		enc := expfmt.NewEncoder(&gotFormatted, expfmt.NewFormat(expfmt.TypeTextPlain))
		for _, mf := range gather {
			err := enc.Encode(mf)
			assert.NoError(t, err)
		}

		//fmt.Println(gotFormatted.String())
	})
}

func TestParseFullMethodName(t *testing.T) {
	tests := []struct {
		name        string
		fullMethod  string
		wantService string
		wantMethod  string
	}{
		{
			name:        "valid method with leading slash",
			fullMethod:  "/service/method",
			wantService: "service",
			wantMethod:  "method",
		},
		{
			name:        "valid method without leading slash",
			fullMethod:  "service/method",
			wantService: "service",
			wantMethod:  "method",
		},
		{
			name:        "invalid format",
			fullMethod:  "invalid",
			wantService: "unknown",
			wantMethod:  "unknown",
		},
		{
			name:        "empty string",
			fullMethod:  "",
			wantService: "unknown",
			wantMethod:  "unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			service, method := parseFullMethodName(tt.fullMethod)
			assert.Equal(t, tt.wantService, service)
			assert.Equal(t, tt.wantMethod, method)
		})
	}
}

func TestDurationBucket(t *testing.T) {
	tests := []struct {
		name     string
		duration time.Duration
		want     string
	}{
		{"less than 1s", 500 * time.Millisecond, "0s_1s"},
		{"between 1s and 10s", 5 * time.Second, "1s_10s"},
		{"between 10s and 30s", 20 * time.Second, "10s_30s"},
		{"between 30s and 1m", 45 * time.Second, "30s_1m"},
		{"between 1m and 10m", 5 * time.Minute, "1m_10m"},
		{"between 10m and 15m", 12 * time.Minute, "10m_15m"},
		{"between 15m and 30m", 20 * time.Minute, "10m_30m"},
		{"between 30m and 1h", 45 * time.Minute, "30m_1h"},
		{"between 1h and 6h", 3 * time.Hour, "1h_6h"},
		{"between 6h and 12h", 8 * time.Hour, "6h_12h"},
		{"between 12h and 24h", 18 * time.Hour, "12h_24h"},
		{"over 24h", 25 * time.Hour, "24h_over"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := durationBucket(tt.duration)
			assert.Equal(t, tt.want, got)
		})
	}
}
