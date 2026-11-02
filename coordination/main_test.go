package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

func TestFSMStrictAllowlistAndRestore(t *testing.T) {
	f := &fsm{data: map[string]string{}}
	for _, input := range []string{`{"key":"leases","value":"x"}`, `{"key":"jobs","value":"x"}`, `{"key":"operator_note","value":"` + strings.Repeat("x", 1025) + `"}`} {
		if f.Apply(&raft.Log{Data: []byte(input)}) == nil {
			t.Fatalf("accepted %s", input)
		}
	}
	if err := f.Apply(&raft.Log{Data: []byte(`{"key":"cluster_label","value":"lab"}`)}); err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(f.copy())
	restored := &fsm{data: map[string]string{}}
	if err := restored.Restore(io.NopCloser(bytes.NewReader(b))); err != nil {
		t.Fatal(err)
	}
	if restored.copy()["cluster_label"] != "lab" {
		t.Fatal("restore lost value")
	}
	if err := restored.Restore(io.NopCloser(strings.NewReader(`{"leases":"x"}`))); err == nil {
		t.Fatal("accepted unsafe snapshot")
	}
	// JSON escaping can expand each permitted UTF-8 byte sixfold. A valid
	// maximum-size snapshot must remain restorable after '<', '>' or '&'.
	expanded, _ := json.Marshal(map[string]string{
		"cluster_label": strings.Repeat("<", 1024),
		"operator_note": strings.Repeat("&", 1024),
	})
	if err := restored.Restore(io.NopCloser(bytes.NewReader(expanded))); err != nil {
		t.Fatal(err)
	}
	if err := restored.Restore(io.NopCloser(strings.NewReader(`{"cluster_label":"x"} {}`))); err == nil {
		t.Fatal("accepted trailing snapshot data")
	}
}

type testNode struct {
	r     *raft.Raft
	tr    *raft.InmemTransport
	store *raftboltdb.BoltStore
	f     *fsm
}

func startTestNode(t *testing.T, id string, dir string) *testNode {
	t.Helper()
	store, err := raftboltdb.NewBoltStore(filepath.Join(dir, "raft.db"))
	if err != nil {
		t.Fatal(err)
	}
	snapshots, err := raft.NewFileSnapshotStore(dir, 2, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	_, tr := raft.NewInmemTransport(raft.ServerAddress(id))
	cfg := raft.DefaultConfig()
	cfg.LocalID = raft.ServerID(id)
	cfg.HeartbeatTimeout = 250 * time.Millisecond
	cfg.ElectionTimeout = 250 * time.Millisecond
	cfg.LeaderLeaseTimeout = 125 * time.Millisecond
	cfg.CommitTimeout = 25 * time.Millisecond
	cfg.LogOutput = io.Discard
	f := &fsm{data: map[string]string{}}
	r, err := raft.NewRaft(cfg, f, store, store, snapshots, tr)
	if err != nil {
		t.Fatal(err)
	}
	return &testNode{r, tr, store, f}
}
func (n *testNode) close() { _ = n.r.Shutdown().Error(); _ = n.tr.Close(); _ = n.store.Close() }
func connect(a, b *testNode) {
	a.tr.Connect(b.tr.LocalAddr(), b.tr)
	b.tr.Connect(a.tr.LocalAddr(), a.tr)
}
func disconnect(a, b *testNode) { a.tr.Disconnect(b.tr.LocalAddr()); b.tr.Disconnect(a.tr.LocalAddr()) }
func waitFor(t *testing.T, test func() bool) {
	t.Helper()
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		if test() {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatal("timed out")
}
func TestThreeVoterPartitionAndDurableRestart(t *testing.T) {
	dirs := []string{t.TempDir(), t.TempDir(), t.TempDir()}
	nodes := make([]*testNode, 3)
	for i := range nodes {
		nodes[i] = startTestNode(t, fmt.Sprint(i), dirs[i])
	}
	defer func() {
		for _, n := range nodes {
			if n != nil {
				n.close()
			}
		}
	}()
	for i := 0; i < 3; i++ {
		for j := i + 1; j < 3; j++ {
			connect(nodes[i], nodes[j])
		}
	}
	servers := []raft.Server{}
	for i := 0; i < 3; i++ {
		id := fmt.Sprint(i)
		servers = append(servers, raft.Server{ID: raft.ServerID(id), Address: raft.ServerAddress(id)})
	}
	if err := nodes[0].r.BootstrapCluster(raft.Configuration{Servers: servers}).Error(); err != nil {
		t.Fatal(err)
	}
	leader := -1
	waitFor(t, func() bool {
		for i, n := range nodes {
			if n.r.State() == raft.Leader {
				leader = i
				return true
			}
		}
		return false
	})
	if err := nodes[leader].r.Apply([]byte(`{"key":"cluster_label","value":"before"}`), time.Second).Error(); err != nil {
		t.Fatal(err)
	}
	follower := (leader + 1) % 3
	token := strings.Repeat("a", 32)
	a := &api{node: nodes[follower].r, fsm: nodes[follower].f, token: token, adminAddr: "127.0.0.1:8080"}
	req := httptest.NewRequest(http.MethodGet, "http://127.0.0.1:8080/v1/status", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	a.ServeHTTP(w, req)
	var status struct {
		Quorum              *bool `json:"quorum"`
		LocalWriteAvailable bool  `json:"local_write_available"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &status); err != nil || status.Quorum != nil || status.LocalWriteAvailable {
		t.Fatalf("follower must report unknown quorum and no local writes: %s", w.Body)
	}
	minority := leader
	others := []int{}
	for i := range nodes {
		if i != minority {
			others = append(others, i)
			disconnect(nodes[minority], nodes[i])
		}
	}
	newLeader := -1
	waitFor(t, func() bool {
		for _, i := range others {
			if nodes[i].r.State() == raft.Leader {
				newLeader = i
				return true
			}
		}
		return false
	})
	if err := nodes[minority].r.Apply([]byte(`{"key":"cluster_label","value":"bad"}`), time.Second).Error(); err == nil {
		t.Fatal("minority committed")
	}
	if err := nodes[newLeader].r.Apply([]byte(`{"key":"operator_note","value":"after"}`), time.Second).Error(); err != nil {
		t.Fatal(err)
	}
	for _, i := range others {
		connect(nodes[minority], nodes[i])
	}
	waitFor(t, func() bool { return nodes[minority].f.copy()["operator_note"] == "after" })
	// Stop the other voters. The survivor must not create a new cluster or commit.
	for _, i := range others {
		nodes[i].close()
		nodes[i] = nil
	}
	waitFor(t, func() bool { return nodes[minority].r.State() != raft.Leader })
	if err := nodes[minority].r.Apply([]byte(`{"key":"cluster_label","value":"bad"}`), time.Second).Error(); err == nil {
		t.Fatal("single survivor committed")
	}
	nodes[minority].close()
	nodes[minority] = nil
	// Restart all voters from their original persistent logs, without bootstrap.
	for i := range nodes {
		nodes[i] = startTestNode(t, fmt.Sprint(i), dirs[i])
		if err := nodes[i].r.BootstrapCluster(raft.Configuration{Servers: servers}).Error(); err == nil {
			t.Fatal("existing state accepted rebootstrap")
		}
	}
	for i := 0; i < 3; i++ {
		for j := i + 1; j < 3; j++ {
			connect(nodes[i], nodes[j])
		}
	}
	waitFor(t, func() bool {
		for _, n := range nodes {
			if n.f.copy()["operator_note"] != "after" || n.f.copy()["cluster_label"] != "before" {
				return false
			}
		}
		return true
	})
}

func TestAdminRequestBoundaries(t *testing.T) {
	a := &api{token: strings.Repeat("a", 32), adminAddr: "127.0.0.1:8080"}
	tests := []struct {
		host, origin, fetch, token, body string
		want                             int
	}{
		{"evil.example:8080", "", "", strings.Repeat("a", 32), `{}`, 403},
		{"127.0.0.1:8080", "https://evil.example", "", strings.Repeat("a", 32), `{}`, 403},
		{"127.0.0.1:8080", "", "cross-site", strings.Repeat("a", 32), `{}`, 403},
		{"127.0.0.1:8080", "", "", "wrong", `{}`, 401},
		{"127.0.0.1:8080", "", "", strings.Repeat("a", 32), `{"key":"jobs","value":"x"}`, 400},
		{"127.0.0.1:8080", "", "", strings.Repeat("a", 32), `{"key":"cluster_label","value":"x","extra":1}`, 400},
	}
	for _, tc := range tests {
		r := httptest.NewRequest(http.MethodPut, "http://"+tc.host+"/v1/metadata", strings.NewReader(tc.body))
		r.Header.Set("Authorization", "Bearer "+tc.token)
		if tc.origin != "" {
			r.Header.Set("Origin", tc.origin)
		}
		if tc.fetch != "" {
			r.Header.Set("Sec-Fetch-Site", tc.fetch)
		}
		w := httptest.NewRecorder()
		a.ServeHTTP(w, r)
		if w.Code != tc.want {
			t.Fatalf("got %d want %d", w.Code, tc.want)
		}
	}
}

func TestSingleVoterAPIAndDurableRestart(t *testing.T) {
	dir := t.TempDir()
	n := startTestNode(t, "single", dir)
	defer func() {
		if n != nil {
			n.close()
		}
	}()
	config := raft.Configuration{Servers: []raft.Server{{ID: "single", Address: "single"}}}
	if err := n.r.BootstrapCluster(config).Error(); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return n.r.State() == raft.Leader })
	token := strings.Repeat("a", 32)
	a := &api{node: n.r, fsm: n.f, id: "single", token: token, adminAddr: "127.0.0.1:8080"}
	request := func(method, path, body string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(method, "http://127.0.0.1:8080"+path, strings.NewReader(body))
		r.Header.Set("Authorization", "Bearer "+token)
		w := httptest.NewRecorder()
		a.ServeHTTP(w, r)
		return w
	}
	if w := request(http.MethodPut, "/v1/metadata", `{"key":"cluster_label","value":"lab"}`); w.Code != 200 {
		t.Fatalf("PUT: %d %s", w.Code, w.Body)
	}
	if w := request(http.MethodGet, "/v1/status", ""); w.Code != 200 || !strings.Contains(w.Body.String(), `"quorum":true`) {
		t.Fatalf("status: %d %s", w.Code, w.Body)
	}
	n.close()
	n = nil
	n = startTestNode(t, "single", dir)
	waitFor(t, func() bool { return n.r.State() == raft.Leader })
	a.node = n.r
	a.fsm = n.f
	if w := request(http.MethodGet, "/v1/metadata", ""); w.Code != 200 || !strings.Contains(w.Body.String(), `"cluster_label":"lab"`) || !strings.Contains(w.Body.String(), `"consistent":true`) {
		t.Fatalf("recovered: %d %s", w.Code, w.Body)
	}
}
