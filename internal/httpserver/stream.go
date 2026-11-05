package httpserver

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/pos-term/payment-processor/internal/events"
)

// EventSource reads the shared replay journal; all instances use the same source.
type EventSource interface {
	Read(context.Context, string) (events.Batch, error)
}

type Option func(*api)

func WithEventSource(source EventSource) Option { return func(a *api) { a.events = source } }

const keepAliveInterval = 15 * time.Second

func (a *api) streamTransactions(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	terminal, status := q.Get("terminal_id"), q.Get("status")
	if (q.Has("terminal_id") && !terminalIDRe.MatchString(terminal)) || len(q["terminal_id"]) > 1 {
		writeProblem(w, http.StatusBadRequest, codeInvalidParameter, "terminal_id must match "+terminalIDRe.String())
		return
	}
	if q.Has("status") && (len(q["status"]) != 1 || (status != "pending" && status != "success" && status != "failed")) {
		writeProblem(w, http.StatusBadRequest, codeInvalidParameter, "status must be one of pending, success, failed")
		return
	}
	cursor := r.Header.Get("Last-Event-ID")
	if len(cursor) > 256 {
		writeProblem(w, http.StatusBadRequest, codeInvalidParameter, "Last-Event-ID is too long")
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	rc := http.NewResponseController(w)
	write := func(frame string) bool {
		// A slow or disconnected client must not pin a handler or delay shutdown.
		_ = rc.SetWriteDeadline(time.Now().Add(5 * time.Second))
		if _, err := fmt.Fprint(w, frame); err != nil {
			return false
		}
		return rc.Flush() == nil
	}
	sendReset := func(id string) bool {
		frame := "event: resync\ndata: {}\n\n"
		if id != "" {
			frame = "id: " + id + "\n" + frame
		}
		return write(frame)
	}
	poll := time.NewTicker(250 * time.Millisecond)
	defer poll.Stop()
	interval := a.keepAlive
	if interval == 0 {
		interval = keepAliveInterval
	}
	heartbeat := time.NewTicker(interval)
	defer heartbeat.Stop()
	connected, unavailable, recovered := false, false, false
	for {
		if r.Context().Err() != nil {
			return
		}
		var batch events.Batch
		var err error
		if a.events == nil {
			err = fmt.Errorf("SSE source is not configured")
		} else {
			batch, err = a.events.Read(r.Context(), cursor)
		}
		if r.Context().Err() != nil {
			return
		}
		if !connected {
			if !write(": connected\n\n") {
				return
			}
			connected = true
		}
		if err != nil {
			if !unavailable {
				a.log.Warn("SSE journal unavailable", "error", err)
				if !sendReset("") {
					return
				}
			}
			unavailable = true
		} else {
			if unavailable {
				// Reload after recovery as writes may have been lost during the outage.
				cursor = ""
				unavailable = false
				recovered = true
				continue
			}
			cursor = batch.Cursor
			if batch.Resync || recovered {
				recovered = false
				if !sendReset(cursor) {
					return
				}
			}
			for _, event := range batch.Events {
				if event.Kind == "resync" {
					if !sendReset(event.ID) {
						return
					}
				} else if event.Kind == "transaction.updated" && (terminal == "" || event.Transaction.TerminalID == terminal) && (status == "" || event.Transaction.Status == status) {
					data, marshalErr := json.Marshal(toJSON(event.Transaction))
					if marshalErr != nil {
						a.log.Error("encode SSE transaction", "error", marshalErr)
						return
					}
					if !write("id: " + event.ID + "\nevent: transaction.updated\ndata: " + string(data) + "\n\n") {
						return
					}
				}
			}
			if cursor != "" && len(batch.Events) > 0 {
				select {
				case <-heartbeat.C:
					if !write(": keep-alive\n\n") {
						return
					}
				default:
				}
				continue
			}
		}
		select {
		case <-r.Context().Done():
			return
		case <-heartbeat.C:
			if !write(": keep-alive\n\n") {
				return
			}
		case <-poll.C:
		}
	}
}
