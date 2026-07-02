package review

import (
	"context"
	"time"

	"github.com/WindyRivers/moderate-pipe/pkg/logger"
	userpb "github.com/WindyRivers/moderate-pipe/proto/gen"
	"github.com/sony/gobreaker"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// defaultReputation is the value assumed when the User Service can't be reached.
// It sits above reputationFloor so degradation is fail-open: an outage of a
// non-critical dependency must not silently send every post to manual review.
const defaultReputation = 80

// UserClient wraps the gRPC UserService client with three layers of fault
// tolerance stacked from inside out:
//
//	call  -> per-attempt deadline (grpc timeout)
//	retry -> exponential backoff over transient failures
//	breaker (gobreaker) -> after enough consecutive failures it opens and
//	         fast-fails, so we stop hammering a service that is clearly down;
//	         after a cool-off it half-opens to probe recovery.
//
// When all of that still yields no answer, GetReputation returns the default
// with degraded=true instead of an error — the caller degrades gracefully
// rather than dead-lettering the message. Reserving the DLQ for genuine poison
// messages (not dependency outages) keeps the pipeline flowing during an
// incident, which is the whole point of the fallback requirement.
type UserClient struct {
	conn    *grpc.ClientConn
	client  userpb.UserServiceClient
	breaker *gobreaker.CircuitBreaker
}

// NewUserClient dials the User Service lazily (grpc.NewClient does not block) so
// the Review Service can start before the User Service is up.
func NewUserClient(addr string) (*UserClient, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	cb := gobreaker.NewCircuitBreaker(gobreaker.Settings{
		Name:        "user-service",
		MaxRequests: 1,               // in half-open, allow a single probe
		Interval:    60 * time.Second, // window over which failure counts reset
		Timeout:     10 * time.Second, // how long to stay open before half-open
		ReadyToTrip: func(c gobreaker.Counts) bool {
			// Trip once we've seen a handful of requests and >60% failed. This
			// avoids tripping on a single blip while reacting quickly to a real
			// outage.
			return c.Requests >= 5 && float64(c.TotalFailures)/float64(c.Requests) > 0.6
		},
		OnStateChange: func(name string, from, to gobreaker.State) {
			logger.L().Warn("circuit breaker state change",
				zap.String("breaker", name),
				zap.String("from", from.String()), zap.String("to", to.String()))
		},
	})
	return &UserClient{conn: conn, client: userpb.NewUserServiceClient(conn), breaker: cb}, nil
}

// GetReputation returns (reputation, degraded). degraded=true means the value is
// the fallback default and the reputation gate should be skipped.
func (uc *UserClient) GetReputation(ctx context.Context, userID uint) (int, bool) {
	res, err := uc.breaker.Execute(func() (interface{}, error) {
		return uc.callWithRetry(ctx, userID)
	})
	if err != nil {
		logger.L().Warn("reputation lookup failed, degrading to default",
			zap.Uint("user_id", userID), zap.Error(err))
		return defaultReputation, true
	}
	return res.(int), false
}

// callWithRetry does up to 3 attempts with exponential backoff (100ms, 200ms),
// each attempt bounded by a 500ms deadline so a hung server can't stall the
// worker.
func (uc *UserClient) callWithRetry(ctx context.Context, userID uint) (int, error) {
	const attempts = 3
	backoff := 100 * time.Millisecond
	var lastErr error
	for i := 0; i < attempts; i++ {
		callCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
		resp, err := uc.client.GetUserReputation(callCtx, &userpb.GetUserReputationRequest{
			UserId: uint64(userID),
		})
		cancel()
		if err == nil {
			return int(resp.GetReputation()), nil
		}
		lastErr = err
		if i < attempts-1 {
			select {
			case <-time.After(backoff):
				backoff *= 2
			case <-ctx.Done():
				return 0, ctx.Err()
			}
		}
	}
	return 0, lastErr
}

// Close releases the gRPC connection.
func (uc *UserClient) Close() error { return uc.conn.Close() }
