package main

import (
	"bytes"
	"encoding/json"
)

// Span is a run of same-kind SSE events on the request timeline (milliseconds since the request arrived).
type Span struct {
	K string `json:"k"` // reason | text | tool | usage
	S int    `json:"s"`
	E int    `json:"e"`
	N int    `json:"n"` // chunks
	B int    `json:"b"` // bytes
}

// Trace is the stored timing breakdown of a streamed response.
type Trace struct {
	Spans   []Span `json:"spans"`
	Chunks  int    `json:"chunks"`
	Bytes   int    `json:"bytes"`
	FirstMs int    `json:"first_ms"` // first reasoning/text/tool chunk
	MaxGap  int    `json:"max_gap_ms"`
	Finish  string `json:"finish"`
	Done    bool   `json:"done"` // saw [DONE] or a terminal response.* event
}

const (
	maxSpans   = 300
	mergeGapMs = 250
	maxPending = 1 << 20
)

type tracer struct {
	t       Trace
	pending []byte
	last    int // ms of the previous chunk
}

type sseEvent struct {
	Type    string          `json:"type"`
	Usage   json.RawMessage `json:"usage"`
	Choices []struct {
		Delta struct {
			Content          json.RawMessage `json:"content"`
			ReasoningContent json.RawMessage `json:"reasoning_content"`
			Reasoning        json.RawMessage `json:"reasoning"`
			ToolCalls        json.RawMessage `json:"tool_calls"`
		} `json:"delta"`
		FinishReason *string `json:"finish_reason"`
	} `json:"choices"`
	Response *struct {
		Status            string `json:"status"`
		IncompleteDetails *struct {
			Reason string `json:"reason"`
		} `json:"incomplete_details"`
		Output []struct {
			Type string `json:"type"`
		} `json:"output"`
	} `json:"response"`
}

func nonEmpty(r json.RawMessage) bool {
	s := string(bytes.TrimSpace(r))
	return s != "" && s != "null" && s != `""` && s != "[]"
}

// feed consumes a chunk of the response body read at ms milliseconds after the request started.
func (tr *tracer) feed(p []byte, ms int) {
	tr.pending = append(tr.pending, p...)
	for {
		i := bytes.IndexByte(tr.pending, '\n')
		if i < 0 {
			break
		}
		line := bytes.TrimSpace(tr.pending[:i])
		tr.pending = tr.pending[i+1:]
		tr.line(line, i+1, ms)
	}
	if len(tr.pending) > maxPending {
		tr.pending = nil
	}
}

func (tr *tracer) line(l []byte, n, ms int) {
	if !bytes.HasPrefix(l, []byte("data:")) {
		return
	}
	l = bytes.TrimSpace(l[5:])
	if len(l) == 0 {
		return
	}
	tr.t.Chunks++
	tr.t.Bytes += n
	if tr.t.Chunks > 1 && ms-tr.last > tr.t.MaxGap {
		tr.t.MaxGap = ms - tr.last
	}
	tr.last = ms
	if string(l) == "[DONE]" {
		tr.t.Done = true
		return
	}
	var e sseEvent
	if json.Unmarshal(l, &e) != nil {
		return
	}
	kind := ""
	for _, c := range e.Choices {
		switch {
		case nonEmpty(c.Delta.ReasoningContent) || nonEmpty(c.Delta.Reasoning):
			kind = "reason"
		case nonEmpty(c.Delta.ToolCalls):
			kind = "tool"
		case nonEmpty(c.Delta.Content):
			kind = "text"
		}
		if c.FinishReason != nil && *c.FinishReason != "" {
			tr.t.Finish = *c.FinishReason
		}
	}
	switch e.Type {
	case "response.output_text.delta":
		kind = "text"
	case "response.reasoning_summary_text.delta", "response.reasoning_text.delta":
		kind = "reason"
	case "response.function_call_arguments.delta":
		kind = "tool"
	case "response.completed", "response.incomplete", "response.failed":
		tr.t.Done = true
		if e.Response != nil {
			tr.t.Finish = responsesFinish(e.Type, e.Response.Status, e.Response.IncompleteDetails, e.Response.Output)
		}
	}
	if kind == "" && nonEmpty(e.Usage) {
		kind = "usage"
	}
	if kind == "" {
		return
	}
	if kind != "usage" && kind != "" && tr.t.FirstMs == 0 {
		tr.t.FirstMs = ms
	}
	tr.add(kind, ms, n)
}

func (tr *tracer) add(kind string, ms, n int) {
	sp := tr.t.Spans
	if k := len(sp) - 1; k >= 0 && sp[k].K == kind && (ms-sp[k].E < mergeGapMs || len(sp) >= maxSpans) {
		sp[k].E, sp[k].N, sp[k].B = ms, sp[k].N+1, sp[k].B+n
		return
	}
	if len(sp) >= maxSpans {
		return
	}
	tr.t.Spans = append(sp, Span{K: kind, S: ms, E: ms, N: 1, B: n})
}

func responsesFinish(typ, status string, inc *struct {
	Reason string `json:"reason"`
}, out []struct {
	Type string `json:"type"`
}) string {
	switch {
	case typ == "response.failed" || status == "failed":
		return "error"
	case typ == "response.incomplete" || status == "incomplete":
		if inc != nil && inc.Reason == "max_output_tokens" {
			return "length"
		}
		if inc != nil && inc.Reason != "" {
			return inc.Reason
		}
		return "incomplete"
	}
	for _, o := range out {
		if o.Type == "function_call" {
			return "tool_calls"
		}
	}
	return "stop"
}

func (tr *tracer) json() string {
	b, _ := json.Marshal(tr.t)
	return string(b)
}

// finishFromBody extracts the stop reason from a non-streamed JSON response.
func finishFromBody(b []byte) string {
	var wrap struct {
		Choices []struct {
			FinishReason string `json:"finish_reason"`
		} `json:"choices"`
		Status            string `json:"status"`
		IncompleteDetails *struct {
			Reason string `json:"reason"`
		} `json:"incomplete_details"`
		Output []struct {
			Type string `json:"type"`
		} `json:"output"`
	}
	if json.Unmarshal(b, &wrap) != nil {
		return ""
	}
	if len(wrap.Choices) > 0 {
		return wrap.Choices[0].FinishReason
	}
	if wrap.Status != "" {
		return responsesFinish("", wrap.Status, wrap.IncompleteDetails, wrap.Output)
	}
	return ""
}
