package log_kafka

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/cenkalti/backoff/v4"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
	"golang.org/x/net/context"
)

func init() {
	caddy.RegisterModule(KafkaLogger{})
}

// Reference: https://github.com/caddyserver/caddy/blob/e3c369d4526e44f23efb10aaad8a60ce519720a0/modules/logging/netwriter.go
type KafkaLogger struct {
	logger    *zap.Logger
	Leader    string `json:"leader"`
	Topic     string `json:"topic"`
	Partition int64  `json:"partition"`
}

// CaddyModule returns the Caddy module information.
func (KafkaLogger) CaddyModule() caddy.ModuleInfo {
	// https://github.com/caddyserver/caddy/blob/c48fadc4a7655008d13076c7f757c36368e2ca13/caddyconfig/httpcaddyfile/builtins.go#L702
	return caddy.ModuleInfo{
		ID:  "caddy.logging.writers.kafka",
		New: func() caddy.Module { return new(KafkaLogger) },
	}
}

func (k *KafkaLogger) Cleanup() error {
	return nil
}

func (k *KafkaLogger) Provision(ctx caddy.Context) error {
	k.logger = ctx.Logger(k)
	return nil
}

// Validate implements caddy.Validator.
func (k *KafkaLogger) Validate() error {
	return nil
}

// UnmarshalCaddyfile implements caddyfile.Unmarshaler.
func (k *KafkaLogger) UnmarshalCaddyfile(d *caddyfile.Dispenser) error {
	for d.Next() {
		for nesting := d.Nesting(); d.NextBlock(nesting); {
			switch d.Val() {
			case "leader":
				if !d.Args(&k.Leader) {
					return d.ArgErr()
				}
			case "topic":
				if !d.Args(&k.Topic) {
					return d.ArgErr()
				}
			case "partition":
				{
					var partition string
					if !d.Args(&partition) {
						return d.ArgErr()
					}
					var v int64
					var err error
					if v, err = strconv.ParseInt(partition, 10, 64); err != nil {
						return err
					}
					k.Partition = v
				}
			default:
				return d.ArgErr()
			}
		}
	}

	return nil
}

func (k *KafkaLogger) WriterKey() string {
	return fmt.Sprintf("kafka:%s,%s,%d", string2json(k.Leader), string2json(k.Topic), k.Partition)
}

func (k *KafkaLogger) String() string {
	return k.WriterKey()
}

func (k *KafkaLogger) OpenWriter() (io.WriteCloser, error) {
	worker := make(chan []byte, 65536)
	w := &LogWriter{
		k:  k,
		tx: worker,
	}
	go k.runWorker(worker)
	return w, nil
}

func (k *KafkaLogger) runWorker(worker <-chan []byte) {
	dial := func() (*kafka.Conn, error) {
		ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
		return kafka.DialLeader(ctx, "tcp", k.Leader, k.Topic, int(k.Partition))
	}

	var conn *kafka.Conn
	defer func() {
		if conn != nil {
			conn.Close()
		}
	}()

	for {
		if conn == nil {
			backoffStrategy := backoff.NewExponentialBackOff()
			backoffStrategy.MaxElapsedTime = 0
			err := backoff.Retry(func() error {
				var err error
				conn, err = dial()
				return err
			}, backoffStrategy)
			if err != nil {
				panic("backoff retry still returns error")
			}
			k.logger.Info("kafka connection established")
		}

		d := <-worker
		if d == nil {
			break
		}
		messages := []kafka.Message{
			{Value: d},
		}

		// Burst
	outer:
		for i := 0; i < 999; i++ {
			select {
			case d := <-worker:
				if d == nil {
					break outer
				}
				messages = append(messages, kafka.Message{
					Value: d,
				})
			default:
				break outer
			}
		}
		if _, err := conn.WriteMessages(messages...); err != nil {
			conn.Close()
			conn = nil
			k.logger.Error("kafka write failed", zap.Error(err))
			for _, msg := range messages {
				os.Stderr.WriteString(fmt.Sprintf("kafka log delivery failed: %s\n", string(msg.Value)))
			}
		}
	}
}

type LogWriter struct {
	k  *KafkaLogger
	tx chan<- []byte
}

func (w *LogWriter) sendToBackend(data []byte) {
	select {
	case w.tx <- data:
	default:
		os.Stderr.Write(data)
	}
}

func (w *LogWriter) Write(p []byte) (n int, err error) {
	w.sendToBackend(append([]byte{}, p...))
	return len(p), nil
}

func (w *LogWriter) Close() error {
	close(w.tx)
	return nil
}

func string2json(s string) string {
	res, err := json.Marshal(s)
	if err != nil {
		panic(err)
	}
	return string(res)
}

var (
	_ caddy.Provisioner     = (*KafkaLogger)(nil)
	_ caddy.Validator       = (*KafkaLogger)(nil)
	_ caddyfile.Unmarshaler = (*KafkaLogger)(nil)
	_ caddy.WriterOpener    = (*KafkaLogger)(nil)
	_ caddy.CleanerUpper    = (*KafkaLogger)(nil)
	_ io.WriteCloser        = (*LogWriter)(nil)
)
