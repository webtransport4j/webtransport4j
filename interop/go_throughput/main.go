package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/webtransport-go"
)

func main() {
	url := "https://127.0.0.1:4433/test"
	if len(os.Args) > 2 {
		url = os.Args[2]
	}

	testNum := 0
	if len(os.Args) > 1 {
		if val, err := strconv.Atoi(os.Args[1]); err == nil {
			testNum = val
			fmt.Printf("🎯 Running Test Option: #%d\n", testNum)
		}
	}

	fmt.Println("============================================================")
	fmt.Println("🚀 WebTransport Go Throughput Benchmark 🚀")
	fmt.Println("============================================================")

	// Configure Dialer with large flow control windows
	dialer := &webtransport.Dialer{
		TLSClientConfig: &tls.Config{
			NextProtos:         []string{"h3"},
			InsecureSkipVerify: true,
		},
		QUICConfig: &quic.Config{
			EnableStreamResetPartialDelivery: true,
			EnableDatagrams:                true,
			InitialStreamReceiveWindow:     256 * 1024 * 1024,  // 256 MB
			MaxStreamReceiveWindow:         1024 * 1024 * 1024, // 1 GB
			InitialConnectionReceiveWindow: 512 * 1024 * 1024,  // 512 MB
			MaxConnectionReceiveWindow:     2048 * 1024 * 1024, // 2 GB
		},
	}
	defer dialer.Close()

	if testNum == 0 || testNum == 1 {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		_, sess, err := dialer.Dial(ctx, url, nil)
		cancel()
		if err != nil {
			log.Fatalf("Test #1 connection failed: %v", err)
		}
		if err := runStreamThroughputTest(sess, 5*time.Second, 8); err != nil {
			log.Printf("Test #1 failed: %v", err)
		}
		sess.CloseWithError(0, "")
		time.Sleep(200 * time.Millisecond)
	}

	if testNum == 0 || testNum == 2 {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		_, sess, err := dialer.Dial(ctx, url, nil)
		cancel()
		if err != nil {
			log.Fatalf("Test #2 connection failed: %v", err)
		}
		if err := runDatagramOpsTest(sess, 3*time.Second); err != nil {
			log.Printf("Test #2 failed: %v", err)
		}
		sess.CloseWithError(0, "")
		time.Sleep(200 * time.Millisecond)
	}

	if testNum == 0 || testNum == 3 {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		_, sess, err := dialer.Dial(ctx, url, nil)
		cancel()
		if err != nil {
			log.Fatalf("Test #3 connection failed: %v", err)
		}
		if err := runLatencyTest(sess, 16, 50); err != nil {
			log.Printf("Test #3 failed: %v", err)
		}
		sess.CloseWithError(0, "")
	}

	fmt.Println("🎉 BENCHMARK RUN COMPLETED!")
}

func runStreamThroughputTest(sess *webtransport.Session, duration time.Duration, numStreams int) error {
	fmt.Printf("🧪 --- Running Bidirectional Stream Throughput Test (%d streams) ---\n", numStreams)

	var wg sync.WaitGroup
	var totalSent atomic.Int64
	var totalRecv atomic.Int64
	errChan := make(chan error, numStreams*2)

	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	for i := 0; i < numStreams; i++ {
		wg.Add(1)
		go func(streamID int) {
			defer wg.Done()
			stream, err := sess.OpenStream()
			if err != nil {
				errChan <- fmt.Errorf("stream %d open failed: %w", streamID, err)
				return
			}
			defer stream.Close()

			// Init echo handshake
			initMsg := []byte("INIT_BENCHMARK")
			if _, err := stream.Write(initMsg); err != nil {
				errChan <- fmt.Errorf("stream %d init write failed: %w", streamID, err)
				return
			}

			buf := make([]byte, 1024)
			n, err := stream.Read(buf)
			if err != nil {
				errChan <- fmt.Errorf("stream %d init read failed: %w", streamID, err)
				return
			}

			ackStr := string(buf[:n])
			if !strings.Contains(ackStr, "ACK BI: INIT_BENCHMARK") {
				errChan <- fmt.Errorf("stream %d unexpected init ack: %q", streamID, ackStr)
				return
			}

			chunkSize := 128 * 1024 // 128 KB chunks
			payload := make([]byte, chunkSize)
			for j := range payload {
				payload[j] = 0x42
			}

			// Reader loop
			go func() {
				readBuf := make([]byte, 128*1024)
				for {
					select {
					case <-ctx.Done():
						return
					default:
						n, err := stream.Read(readBuf)
						if err != nil {
							if err == io.EOF || ctx.Err() != nil {
								return
							}
							errChan <- err
							return
						}
						totalRecv.Add(int64(n))
					}
				}
			}()

			// Writer loop
			for {
				select {
				case <-ctx.Done():
					return
				default:
					n, err := stream.Write(payload)
					if err != nil {
						if ctx.Err() != nil {
							return
						}
						errChan <- err
						return
					}
					totalSent.Add(int64(n))
				}
			}
		}(i)
	}

	// Wait for duration to complete
	select {
	case <-ctx.Done():
		wg.Wait()
	case err := <-errChan:
		cancel()
		wg.Wait()
		return err
	}

	sentMB := float64(totalSent.Load()) / (1024 * 1024)
	recvMB := float64(totalRecv.Load()) / (1024 * 1024)
	secs := duration.Seconds()

	fmt.Println("📊 --- STREAM THROUGHPUT ---")
	fmt.Printf("📤 Sent:     %.2f MB  (%.2f MB/s  |  %.3f Gbps)\n", sentMB, sentMB/secs, (sentMB*8/1024)/secs)
	fmt.Printf("📥 Received: %.2f MB  (%.2f MB/s  |  %.3f Gbps)\n", recvMB, recvMB/secs, (recvMB*8/1024)/secs)
	fmt.Println("============================================================")
	return nil
}

func runDatagramOpsTest(sess *webtransport.Session, duration time.Duration) error {
	fmt.Println("🧪 --- Running Datagram Ops/Sec Test ---")

	payload := []byte("BenchDG")
	var totalSent atomic.Int64

	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	t0 := time.Now()
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				if err := sess.SendDatagram(payload); err != nil {
					if ctx.Err() != nil {
						return
					}
					log.Printf("Datagram write error: %v", err)
					return
				}
				totalSent.Add(1)
			}
		}
	}()

	<-ctx.Done()
	dt := time.Since(t0).Seconds()
	sentCount := totalSent.Load()
	fmt.Println("📊 --- DATAGRAM OPS ---")
	fmt.Printf("📤 Sent: %d datagrams  (%.2f ops/sec)\n", sentCount, float64(sentCount)/dt)
	fmt.Println("============================================================")
	return nil
}

func runLatencyTest(sess *webtransport.Session, numStreams int, msgs int) error {
	fmt.Printf("🧪 --- Running Latency Test (%d streams × %d msgs) ---\n", numStreams, msgs)

	var latenciesMu sync.Mutex
	var latencies []time.Duration

	var wg sync.WaitGroup
	errChan := make(chan error, numStreams)

	t0 := time.Now()

	for i := 0; i < numStreams; i++ {
		wg.Add(1)
		go func(streamID int) {
			defer wg.Done()
			stream, err := sess.OpenStream()
			if err != nil {
				errChan <- fmt.Errorf("failed to open stream: %w", err)
				return
			}
			defer stream.Close()

			payload := []byte(fmt.Sprintf("Msg %d", streamID))
			buf := make([]byte, 1024)

			for m := 0; m < msgs; m++ {
				start := time.Now()
				if _, err := stream.Write(payload); err != nil {
					errChan <- fmt.Errorf("stream write error: %w", err)
					return
				}

				n, err := stream.Read(buf)
				if err != nil {
					errChan <- fmt.Errorf("stream read error: %w", err)
					return
				}
				_ = n

				elapsed := time.Since(start)

				latenciesMu.Lock()
				latencies = append(latencies, elapsed)
				latenciesMu.Unlock()
			}
		}(i)
	}

	wg.Wait()

	select {
	case err := <-errChan:
		return err
	default:
	}

	totalDuration := time.Since(t0).Seconds()
	totalOps := numStreams * msgs

	latenciesMu.Lock()
	defer latenciesMu.Unlock()

	if len(latencies) == 0 {
		return fmt.Errorf("no latency data collected")
	}

	sort.Slice(latencies, func(i, j int) bool {
		return latencies[i] < latencies[j]
	})

	var sum time.Duration
	for _, l := range latencies {
		sum += l
	}
	avg := sum / time.Duration(len(latencies))

	p50 := latencies[int(float64(len(latencies))*0.50)]
	p95 := latencies[int(float64(len(latencies))*0.95)]
	p99 := latencies[int(float64(len(latencies))*0.99)]
	min := latencies[0]
	max := latencies[len(latencies)-1]

	fmt.Println("📊 --- LATENCY RESULTS ---")
	fmt.Printf("⏱  Avg: %.3f ms | p50: %.3f ms | p95: %.3f ms | p99: %.3f ms\n",
		float64(avg.Microseconds())/1000.0,
		float64(p50.Microseconds())/1000.0,
		float64(p95.Microseconds())/1000.0,
		float64(p99.Microseconds())/1000.0)
	fmt.Printf("⏱  Min: %.3f ms | Max: %.3f ms\n",
		float64(min.Microseconds())/1000.0,
		float64(max.Microseconds())/1000.0)
	fmt.Printf("📈 Per-stream throughput: %.2f ops/sec\n", float64(totalOps)/totalDuration)
	fmt.Println("============================================================")
	return nil
}
