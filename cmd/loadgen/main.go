// Command loadgen is a small closed-loop load generator for the Content
// Service's create-post endpoint. It fires N concurrent workers for a duration,
// then reports throughput and latency percentiles — the raw material for the
// README's load-test section. It also injects a fraction of posts containing a
// sensitive word so the rejection path is exercised under load.
package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"math/rand"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	target := flag.String("target", "http://localhost:8080/posts", "create-post URL")
	concurrency := flag.Int("c", 50, "concurrent workers")
	duration := flag.Duration("d", 30*time.Second, "test duration")
	badRatio := flag.Float64("bad", 0.1, "fraction of posts containing a sensitive word")
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), *duration)
	defer cancel()

	var (
		ok, rejected, failed int64
		latencies            = make([][]time.Duration, *concurrency)
		wg                   sync.WaitGroup
	)

	client := &http.Client{Timeout: 5 * time.Second}
	start := time.Now()

	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)))
			var local []time.Duration
			for ctx.Err() == nil {
				body := makeBody(rng, *badRatio)
				t0 := time.Now()
				req, _ := http.NewRequestWithContext(ctx, http.MethodPost, *target, bytes.NewReader(body))
				req.Header.Set("Content-Type", "application/json")
				resp, err := client.Do(req)
				lat := time.Since(t0)
				if err != nil {
					atomic.AddInt64(&failed, 1)
					continue
				}
				switch resp.StatusCode {
				case http.StatusAccepted:
					atomic.AddInt64(&ok, 1)
					local = append(local, lat)
				case http.StatusTooManyRequests:
					atomic.AddInt64(&rejected, 1)
				default:
					atomic.AddInt64(&failed, 1)
				}
				resp.Body.Close()
			}
			latencies[id] = local
		}(i)
	}
	wg.Wait()
	elapsed := time.Since(start)

	var all []time.Duration
	for _, l := range latencies {
		all = append(all, l...)
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })

	total := ok + rejected + failed
	fmt.Printf("\n=== loadgen report ===\n")
	fmt.Printf("duration:        %s\n", elapsed.Round(time.Millisecond))
	fmt.Printf("concurrency:     %d\n", *concurrency)
	fmt.Printf("requests total:  %d\n", total)
	fmt.Printf("  accepted(202): %d\n", ok)
	fmt.Printf("  ratelimited:   %d\n", rejected)
	fmt.Printf("  failed:        %d\n", failed)
	fmt.Printf("throughput:      %.1f req/s (accepted)\n", float64(ok)/elapsed.Seconds())
	if len(all) > 0 {
		fmt.Printf("latency p50:     %s\n", pct(all, 0.50))
		fmt.Printf("latency p90:     %s\n", pct(all, 0.90))
		fmt.Printf("latency p99:     %s\n", pct(all, 0.99))
		fmt.Printf("latency max:     %s\n", all[len(all)-1].Round(time.Microsecond))
	}
}

func pct(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(p * float64(len(sorted)))
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx].Round(time.Microsecond)
}

func makeBody(rng *rand.Rand, badRatio float64) []byte {
	content := "this is a normal load-test post number " + fmt.Sprint(rng.Intn(1_000_000))
	if rng.Float64() < badRatio {
		content += " buy casino tokens now" // trips the sensitive-word rule
	}
	return []byte(fmt.Sprintf(
		`{"user_id":%d,"title":"load test","content":%q}`,
		1+rng.Intn(3), content))
}
