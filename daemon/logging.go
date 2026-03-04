package daemon

import (
	"context"
	"log/slog"
	"os"

	"github.com/fatih/color"
)

// consoleHandler writes human-friendly log lines to stderr.
type consoleHandler struct {
	level slog.Level
	attrs []slog.Attr
}

func (h *consoleHandler) Enabled(_ context.Context, l slog.Level) bool {
	return l >= h.level
}

var (
	colorTime  = color.New(color.FgHiBlack)
	colorDebug = color.New(color.FgHiBlack)
	colorInfo  = color.New(color.FgCyan)
	colorWarn  = color.New(color.FgYellow)
	colorError = color.New(color.FgRed, color.Bold)
	colorKey   = color.New(color.FgHiBlack)
	colorMsg   = color.New(color.FgWhite, color.Bold)
)

func levelColor(l slog.Level) *color.Color {
	switch {
	case l >= slog.LevelError:
		return colorError
	case l >= slog.LevelWarn:
		return colorWarn
	case l >= slog.LevelInfo:
		return colorInfo
	default:
		return colorDebug
	}
}

func (h *consoleHandler) Handle(_ context.Context, r slog.Record) error {
	var buf []byte
	buf = append(buf, colorTime.Sprint(r.Time.Format("15:04:05"))...)
	buf = append(buf, ' ')
	lc := levelColor(r.Level)
	buf = append(buf, lc.Sprintf("%-5s", r.Level.String())...)
	buf = append(buf, ' ')
	buf = append(buf, colorMsg.Sprint(r.Message)...)
	appendAttr := func(a slog.Attr) {
		buf = append(buf, ' ')
		buf = append(buf, colorKey.Sprint(a.Key)...)
		buf = append(buf, '=')
		buf = append(buf, a.Value.String()...)
	}
	for _, a := range h.attrs {
		appendAttr(a)
	}
	r.Attrs(func(a slog.Attr) bool {
		appendAttr(a)
		return true
	})
	buf = append(buf, '\n')
	_, err := os.Stderr.Write(buf)
	return err
}

func (h *consoleHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &consoleHandler{level: h.level, attrs: append(h.attrs[:len(h.attrs):len(h.attrs)], attrs...)}
}

func (h *consoleHandler) WithGroup(_ string) slog.Handler {
	return h
}

// multiHandler fans out log records to multiple handlers.
type multiHandler []slog.Handler

func (m multiHandler) Enabled(_ context.Context, l slog.Level) bool {
	for _, h := range m {
		if h.Enabled(context.Background(), l) {
			return true
		}
	}
	return false
}

func (m multiHandler) Handle(ctx context.Context, r slog.Record) error {
	for _, h := range m {
		if h.Enabled(ctx, r.Level) {
			if err := h.Handle(ctx, r.Clone()); err != nil {
				return err
			}
		}
	}
	return nil
}

func (m multiHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	handlers := make(multiHandler, len(m))
	for i, h := range m {
		handlers[i] = h.WithAttrs(attrs)
	}
	return handlers
}

func (m multiHandler) WithGroup(name string) slog.Handler {
	handlers := make(multiHandler, len(m))
	for i, h := range m {
		handlers[i] = h.WithGroup(name)
	}
	return handlers
}
