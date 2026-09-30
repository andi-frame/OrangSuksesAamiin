package obs

import (
	"context"
	"log/slog"
	"os"
	"strings"
)

// ctxHandler decorates a slog.Handler so every record logged with a context
// (logger.InfoContext(ctx, ...)) automatically carries the correlation ID as
// the "cid" attribute.
type ctxHandler struct{ slog.Handler }

func (h ctxHandler) Handle(ctx context.Context, r slog.Record) error {
	if id := CorrelationID(ctx); id != "" {
		r.AddAttrs(slog.String("cid", id))
	}
	return h.Handler.Handle(ctx, r)
}

func (h ctxHandler) WithAttrs(a []slog.Attr) slog.Handler {
	return ctxHandler{h.Handler.WithAttrs(a)}
}

func (h ctxHandler) WithGroup(n string) slog.Handler {
	return ctxHandler{h.Handler.WithGroup(n)}
}

// NewLogger builds a JSON logger writing to stdout. Level comes from
// LOG_LEVEL (debug|info|warn|error, default info). Every line carries
// "service" and (when logged with a context) "cid".
func NewLogger(service string) *slog.Logger {
	var lvl slog.Level
	switch strings.ToLower(os.Getenv("LOG_LEVEL")) {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	jh := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: lvl,
		ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
			switch a.Key {
			case slog.TimeKey:
				a.Key = "ts"
			case slog.MessageKey:
				a.Key = "msg"
			}
			return a
		},
	})
	return slog.New(ctxHandler{jh}).With("service", service)
}
