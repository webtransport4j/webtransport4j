package main

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/base64"
	"flag"
	"fmt"
	"io"
	"log"
	"strings"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3/qlog"
	"github.com/quic-go/webtransport-go"
)

func main() {
	if err := runClient(); err != nil {
		log.Fatalf("failed to run client: %v", err)
	}
}

func runClient() error {
	url := flag.String("url", "https://localhost:4433/", "WebTransport URL")
	protocolsFlag := flag.String(
		"protocols",
		"webtransport-test,webtransport-test-2",
		"comma-separated application protocols",
	)
	certHash := flag.String(
		"cert-hash",
		"",
		"base64-encoded SHA-256 server certificate hash",
	)
	insecure := flag.Bool("insecure", false, "skip certificate verification")

	flag.Parse()

	var protocols []string
	for p := range strings.SplitSeq(*protocolsFlag, ",") {
		if p = strings.TrimSpace(p); p != "" {
			protocols = append(protocols, p)
		}
	}

	tlsConf := &tls.Config{
		InsecureSkipVerify: *insecure,
	}

	if hash := strings.TrimSpace(*certHash); hash != "" {
		tlsConf.InsecureSkipVerify = true
		tlsConf.VerifyConnection = func(state tls.ConnectionState) error {
			if len(state.PeerCertificates) == 0 {
				return fmt.Errorf("server didn't send a certificate")
			}

			actualHash := sha256.Sum256(state.PeerCertificates[0].Raw)

			got := base64.RawStdEncoding.EncodeToString(actualHash[:])
			if got != hash {
				return fmt.Errorf(
					"server certificate hash mismatch: got=%s want=%s",
					got,
					hash,
				)
			}

			return nil
		}
	}

	cl := &webtransport.Dialer{
		ApplicationProtocols: protocols,
		TLSClientConfig:      tlsConf,
		QUICConfig: &quic.Config{
			Tracer:                           qlog.DefaultConnectionTracer,
			EnableDatagrams:                  true,
			EnableStreamResetPartialDelivery: true,
		},
	}
	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	rsp, sess, err := cl.Dial(ctx, *url, nil)
	if err != nil {
		return err
	}

	fmt.Println("HTTP status code:", rsp.StatusCode)

	if rsp.StatusCode < 200 || rsp.StatusCode >= 300 {
		return fmt.Errorf("unexpected status: %d", rsp.StatusCode)
	}

	fmt.Printf(
		"negotiated protocol: %s\n",
		sess.SessionState().ApplicationProtocol,
	)

	// ------------------------------------------------------------------
	// Receive server-initiated unidirectional streams
	// ------------------------------------------------------------------

	go func() {
		for {
			str, err := sess.AcceptUniStream(context.Background())
			if err != nil {
				log.Printf("[SERVER UNI] accept error: %v", err)
				return
			}

			go func() {
				data, err := io.ReadAll(str)
				if err != nil {
					log.Printf("[SERVER UNI] read error: %v", err)
					return
				}

				fmt.Printf(
					"[SERVER UNI RX] %s\n",
					string(data),
				)
			}()
		}
	}()

	// ------------------------------------------------------------------
	// Receive server-initiated bidirectional streams
	// ------------------------------------------------------------------

	go func() {
		for {
			str, err := sess.AcceptStream(context.Background())
			if err != nil {
				log.Printf("[SERVER BIDI] accept error: %v", err)
				return
			}

			go func() {
				buf := make([]byte, 4096)

				for {
					n, err := str.Read(buf)
					if err != nil {
						if err != io.EOF {
							log.Printf(
								"[SERVER BIDI] read error: %v",
								err,
							)
						}
						return
					}

					fmt.Printf(
						"[SERVER BIDI RX] %s\n",
						string(buf[:n]),
					)
				}
			}()
		}
	}()

	// ------------------------------------------------------------------
	// Receive datagrams
	// ------------------------------------------------------------------

	go func() {
		for {
			msg, err := sess.ReceiveDatagram(context.Background())
			if err != nil {
				log.Printf("[DATAGRAM RX] error: %v", err)
				return
			}

			fmt.Printf(
				"[DATAGRAM RX] %s\n",
				string(msg),
			)
		}
	}()

	// Give server a moment to start opening streams.
	time.Sleep(time.Second)

	// ------------------------------------------------------------------
	// Client -> Server Unidirectional Stream
	// ------------------------------------------------------------------

	go func() {
		str, err := sess.OpenUniStream()
		if err != nil {
			log.Printf("[CLIENT UNI] open error: %v", err)
			return
		}

		msg := fmt.Sprintf(
			"hello from client uni stream @ %s",
			time.Now().Format(time.RFC3339),
		)

		if _, err := str.Write([]byte(msg)); err != nil {
			log.Printf("[CLIENT UNI] write error: %v", err)
			return
		}

		fmt.Printf("[CLIENT UNI TX] %s\n", msg)

		if err := str.Close(); err != nil {
			log.Printf("[CLIENT UNI] close error: %v", err)
		}
	}()

	// ------------------------------------------------------------------
	// Client -> Server Bidirectional Stream
	// ------------------------------------------------------------------

	go func() {
		str, err := sess.OpenStream()
		if err != nil {
			log.Printf("[CLIENT BIDI] open error: %v", err)
			return
		}

		msg := fmt.Sprintf(
			"hello from client bidi stream @ %s",
			time.Now().Format(time.RFC3339),
		)

		if _, err := str.Write([]byte(msg)); err != nil {
			log.Printf("[CLIENT BIDI] write error: %v", err)
			return
		}

		fmt.Printf("[CLIENT BIDI TX] %s\n", msg)

		buf := make([]byte, 4096)

		n, err := str.Read(buf)
		if err != nil {
			log.Printf("[CLIENT BIDI] response error: %v", err)
			return
		}

		fmt.Printf(
			"[CLIENT BIDI RX] %s\n",
			string(buf[:n]),
		)
	}()

	// ------------------------------------------------------------------
	// Send datagrams periodically
	// ------------------------------------------------------------------

	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()

		counter := 0

		for {
			msg := fmt.Sprintf(
				"client-datagram-%d",
				counter,
			)

			if err := sess.SendDatagram([]byte(msg)); err != nil {
				log.Printf(
					"[DATAGRAM TX] error: %v",
					err,
				)
				return
			}

			fmt.Printf(
				"[DATAGRAM TX] %s\n",
				msg,
			)

			counter++

			<-ticker.C
		}
	}()

	fmt.Println("WebTransport test client running...")
	fmt.Println("Testing:")
	fmt.Println("  ✓ Client Uni Stream")
	fmt.Println("  ✓ Client Bidi Stream")
	fmt.Println("  ✓ Server Uni Stream")
	fmt.Println("  ✓ Server Bidi Stream")
	fmt.Println("  ✓ Datagrams")

	select {}
}
