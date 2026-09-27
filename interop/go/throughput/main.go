package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/quic-go/quic-go"
)


const chunkSize = 64 * 1024 * 10

func sendFile(host string, port int, filename string) error {
	addr := fmt.Sprintf("%s:%d", host, port)

	// EQUIVALENT TO: verify_mode=ssl.CERT_NONE and alpn_protocols=["h3"]
	tlsConf := &tls.Config{
		InsecureSkipVerify: true,
		NextProtos:         []string{"h3"},
	}

	// Connect to the QUIC server
	ctx := context.Background()
	conn, err := quic.DialAddr(ctx, addr, tlsConf, nil)
	if err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}
	// Ensure the connection closes cleanly when the function exits
	defer conn.CloseWithError(0, "client finished")

	// FIX 1 EQUIVALENT: Open a new bidirectional stream synchronously
	stream, err := conn.OpenStreamSync(ctx)
	if err != nil {
		return fmt.Errorf("failed to open stream: %w", err)
	}

	// Open the file for reading
	file, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	// FIX 2 & 3 EQUIVALENT: Read chunks and write to the stream.
	// NOTE ON ASYNC/DRAIN: Go handles I/O blocking gracefully. `io.CopyBuffer`
	// will call `stream.Write()`. If the QUIC flow control window is full,
	// `stream.Write()` automatically blocks this goroutine until window space
	// opens up, preventing RAM bloat naturally. No manual "draining" is required.
	buffer := make([]byte, chunkSize)
	if _, err := io.CopyBuffer(stream, file, buffer); err != nil {
		return fmt.Errorf("failed to send file data: %w", err)
	}

	// FIX 4 EQUIVALENT: Send the FIN bit gracefully.
	// Closing the stream in quic-go closes the write direction and sends EOF.
	if err := stream.Close(); err != nil {
		return fmt.Errorf("failed to close stream cleanly: %w", err)
	}

	fmt.Println("Finished sending.")

	// No need for asyncio.sleep(1); defer conn.CloseWithError() handles the teardown.
	return nil
}

func main() {
	host := "127.0.0.1"
	port := 4242
	filename := "/Users/sam/Documents/GitHub/webtransport4j/interop/large-file.bin"

	if err := sendFile(host, port, filename); err != nil {
		log.Fatalf("Fatal error: %v\n", err)
	}
}