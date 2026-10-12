package main

import "testing"

func TestTracerChat(t *testing.T) {
	tr := &tracer{}
	tr.feed([]byte("data: {\"choices\":[{\"delta\":{\"role\":\"assistant\"}}]}\n\ndata: {\"choices\":[{\"delta\":{\"reasoning_content\":\"hm\"}}]}\n\n"), 100)
	tr.feed([]byte("data: {\"choices\":[{\"delta\":{\"conte"), 200)
	tr.feed([]byte("nt\":\"hi\"}}]}\n\ndata: {\"choices\":[{\"delta\":{},\"finish_reason\":\"tool_calls\"}]}\n\n"), 900)
	tr.feed([]byte("data: {\"choices\":[],\"usage\":{\"prompt_tokens\":1}}\n\ndata: [DONE]\n\n"), 950)
	got := tr.t
	if got.Finish != "tool_calls" || !got.Done || got.FirstMs != 100 || got.MaxGap != 800 {
		t.Fatalf("%+v", got)
	}
	if len(got.Spans) != 3 || got.Spans[0].K != "reason" || got.Spans[1].K != "text" || got.Spans[2].K != "usage" {
		t.Fatalf("spans %+v", got.Spans)
	}
}

func TestTracerResponsesAndBody(t *testing.T) {
	tr := &tracer{}
	tr.feed([]byte("data: {\"type\":\"response.output_text.delta\",\"delta\":\"x\"}\n\ndata: {\"type\":\"response.incomplete\",\"response\":{\"status\":\"incomplete\",\"incomplete_details\":{\"reason\":\"max_output_tokens\"}}}\n\n"), 5)
	if tr.t.Finish != "length" || !tr.t.Done {
		t.Fatalf("%+v", tr.t)
	}
	if f := finishFromBody([]byte(`{"choices":[{"finish_reason":"stop"}]}`)); f != "stop" {
		t.Fatal(f)
	}
	if f := finishFromBody([]byte(`{"status":"completed","output":[{"type":"function_call"}]}`)); f != "tool_calls" {
		t.Fatal(f)
	}
}
