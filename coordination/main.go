// idle-herd-raft is an optional, administrative-only Raft service.
package main

import (
	"crypto/subtle"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)

type command struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func (c command) valid() bool {
	return (c.Key == "cluster_label" || c.Key == "operator_note") && len(c.Value) <= 1024
}

type fsm struct {
	mu   sync.RWMutex
	data map[string]string
}

func (f *fsm) Apply(l *raft.Log) interface{} {
	var c command
	if json.Unmarshal(l.Data, &c) != nil || !c.valid() {
		return errors.New("invalid administrative command")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.data[c.Key] = c.Value
	return nil
}
func (f *fsm) copy() map[string]string {
	f.mu.RLock()
	defer f.mu.RUnlock()
	m := make(map[string]string, len(f.data))
	for k, v := range f.data {
		m[k] = v
	}
	return m
}
func (f *fsm) Snapshot() (raft.FSMSnapshot, error) { return snapshot(f.copy()), nil }
func (f *fsm) Restore(r io.ReadCloser) error {
	defer r.Close()
	var m map[string]string
	decoder := json.NewDecoder(io.LimitReader(r, 16384))
	if err := decoder.Decode(&m); err != nil {
		return err
	}
	var trailing interface{}
	if decoder.Decode(&trailing) != io.EOF {
		return errors.New("invalid snapshot trailing data")
	}
	if m == nil || len(m) > 2 {
		return errors.New("invalid snapshot")
	}
	for k, v := range m {
		if !(command{k, v}).valid() {
			return errors.New("invalid snapshot command")
		}
	}
	f.mu.Lock()
	f.data = m
	f.mu.Unlock()
	return nil
}

type snapshot map[string]string

func (s snapshot) Persist(sink raft.SnapshotSink) error {
	if err := json.NewEncoder(sink).Encode(s); err != nil {
		sink.Cancel()
		return err
	}
	return sink.Close()
}
func (s snapshot) Release() {}

type tlsStream struct {
	net.Listener
	config *tls.Config
}

func (s *tlsStream) Dial(address raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	host, _, err := net.SplitHostPort(string(address))
	if err != nil {
		return nil, err
	}
	c := s.config.Clone()
	c.ServerName = host
	return tls.DialWithDialer(&net.Dialer{Timeout: timeout}, "tcp", string(address), c)
}
func makeTransport(addr, cert, key, ca string) (*raft.NetworkTransport, error) {
	pair, err := tls.LoadX509KeyPair(cert, key)
	if err != nil {
		return nil, err
	}
	pem, err := os.ReadFile(ca)
	if err != nil {
		return nil, err
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(pem) {
		return nil, errors.New("invalid CA")
	}
	cfg := &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{pair}, RootCAs: roots, ClientCAs: roots, ClientAuth: tls.RequireAndVerifyClientCert}
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	return raft.NewNetworkTransport(&tlsStream{tls.NewListener(l, cfg), cfg}, 3, 3*time.Second, os.Stderr), nil
}

type api struct {
	node      *raft.Raft
	fsm       *fsm
	id, token string
	adminAddr string
}

func (a *api) quorum() bool {
	if a.node.State() != raft.Leader {
		return false
	}
	done := make(chan bool, 1)
	future := a.node.VerifyLeader()
	go func() { done <- future.Error() == nil }()
	select {
	case ok := <-done:
		return ok
	case <-time.After(500 * time.Millisecond):
		return false
	}
}
func (a *api) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Host != a.adminAddr || r.Header.Get("Origin") != "" || (r.Header.Get("Sec-Fetch-Site") != "" && r.Header.Get("Sec-Fetch-Site") != "none") {
		http.Error(w, "invalid request origin", http.StatusForbidden)
		return
	}
	if subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+a.token)) != 1 {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	switch {
	case r.URL.Path == "/v1/status" && r.Method == http.MethodGet:
		_, id := a.node.LeaderWithID()
		state := a.node.State()
		var quorum interface{}
		writeAvailable := false
		if state == raft.Leader {
			writeAvailable = a.quorum()
			quorum = writeAvailable
		}
		json.NewEncoder(w).Encode(map[string]interface{}{"node_id": a.id, "state": state.String(), "leader": string(id), "quorum": quorum, "local_write_available": writeAvailable, "commit_index": a.node.CommitIndex()})
	case r.URL.Path == "/v1/metadata" && r.Method == http.MethodGet:
		consistent := a.quorum()
		if consistent {
			consistent = a.node.Barrier(3*time.Second).Error() == nil
		}
		json.NewEncoder(w).Encode(map[string]interface{}{"metadata": a.fsm.copy(), "consistent": consistent})
	case r.URL.Path == "/v1/metadata" && r.Method == http.MethodPut:
		defer r.Body.Close()
		var c command
		d := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096))
		d.DisallowUnknownFields()
		if d.Decode(&c) != nil || !c.valid() {
			http.Error(w, "invalid command", http.StatusBadRequest)
			return
		}
		var extra interface{}
		if d.Decode(&extra) != io.EOF {
			http.Error(w, "invalid command", http.StatusBadRequest)
			return
		}
		if !a.quorum() {
			http.Error(w, "leader or quorum unavailable", http.StatusServiceUnavailable)
			return
		}
		b, _ := json.Marshal(c)
		future := a.node.Apply(b, 3*time.Second)
		if future.Error() != nil || future.Response() != nil {
			http.Error(w, "commit unavailable", http.StatusServiceUnavailable)
			return
		}
		json.NewEncoder(w).Encode(map[string]interface{}{"metadata": a.fsm.copy(), "consistent": true})
	default:
		http.NotFound(w, r)
	}
}

type voters []string

func (v *voters) String() string     { return strings.Join(*v, ",") }
func (v *voters) Set(s string) error { *v = append(*v, s); return nil }
func run() error {
	id := flag.String("id", "", "unique voter ID")
	addr := flag.String("raft-address", "", "raft listen address")
	httpAddr := flag.String("http-address", "", "loopback admin listen address")
	dir := flag.String("data-dir", "", "persistent data directory")
	tokenFile := flag.String("token-file", "", "admin bearer token file")
	cert := flag.String("cert", "", "Raft TLS certificate")
	key := flag.String("key", "", "Raft TLS private key")
	ca := flag.String("ca", "", "Raft trust CA")
	bootstrap := flag.Bool("bootstrap", false, "explicitly initialize an empty cluster")
	var vs voters
	flag.Var(&vs, "voter", "static voter ID=address (repeat)")
	flag.Parse()
	if *id == "" || *addr == "" || *httpAddr == "" || *dir == "" || *tokenFile == "" || *cert == "" || *key == "" || *ca == "" {
		return errors.New("all identity, address, data, token and TLS flags are required")
	}
	host, _, err := net.SplitHostPort(*httpAddr)
	if err != nil || net.ParseIP(host) == nil || !net.ParseIP(host).IsLoopback() {
		return errors.New("admin address must be a loopback IP")
	}
	for _, address := range []string{*addr, *httpAddr} {
		_, port, e := net.SplitHostPort(address)
		if e != nil {
			return e
		}
		n, e := strconv.Atoi(port)
		if e != nil || n < 1 || n > 65535 || (n >= 11434 && n <= 11449) {
			return errors.New("invalid or reserved service port")
		}
	}
	info, err := os.Lstat(*tokenFile)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Size() > 257 || (runtime.GOOS != "windows" && info.Mode().Perm()&0077 != 0) {
		return errors.New("token must be a private regular file")
	}
	tokenBytes, err := os.ReadFile(*tokenFile)
	if err != nil {
		return err
	}
	token := strings.TrimSpace(string(tokenBytes))
	if !regexp.MustCompile(`^[A-Za-z0-9_-]{32,256}$`).MatchString(token) {
		return errors.New("admin token must contain 32 to 256 URL-safe characters")
	}
	if err = os.MkdirAll(*dir, 0700); err != nil {
		return err
	}
	dirInfo, err := os.Lstat(*dir)
	if err != nil {
		return err
	}
	if !dirInfo.IsDir() || (runtime.GOOS != "windows" && dirInfo.Mode().Perm()&0077 != 0) {
		return errors.New("data directory must be a private directory, not a symlink")
	}
	store, err := raftboltdb.NewBoltStore(filepath.Join(*dir, "raft.db"))
	if err != nil {
		return err
	}
	defer store.Close()
	snaps, err := raft.NewFileSnapshotStore(*dir, 3, os.Stderr)
	if err != nil {
		return err
	}
	existing, err := raft.HasExistingState(store, store, snaps)
	if err != nil {
		return err
	}
	var servers []raft.Server
	ids := map[string]bool{}
	addresses := map[string]bool{}
	own := false
	if *bootstrap {
		if existing {
			return errors.New("refusing bootstrap: existing Raft state")
		}
		if len(vs) != 1 && len(vs) != 3 && len(vs) != 5 {
			return errors.New("bootstrap requires exactly 1, 3, or 5 voters")
		}
		for _, v := range vs {
			parts := strings.SplitN(v, "=", 2)
			if len(parts) != 2 || parts[0] == "" || ids[parts[0]] || addresses[parts[1]] {
				return errors.New("invalid or duplicate voter")
			}
			if _, _, err = net.SplitHostPort(parts[1]); err != nil {
				return err
			}
			ids[parts[0]] = true
			addresses[parts[1]] = true
			if parts[0] == *id && parts[1] == *addr {
				own = true
			}
			servers = append(servers, raft.Server{ID: raft.ServerID(parts[0]), Address: raft.ServerAddress(parts[1])})
		}
		if !own {
			return errors.New("bootstrap voters must include this voter and address")
		}
	} else if len(vs) > 0 {
		return errors.New("voters require explicit bootstrap")
	}
	transport, err := makeTransport(*addr, *cert, *key, *ca)
	if err != nil {
		return err
	}
	defer transport.Close()
	cfg := raft.DefaultConfig()
	cfg.LocalID = raft.ServerID(*id)
	f := &fsm{data: make(map[string]string)}
	node, err := raft.NewRaft(cfg, f, store, store, snaps, transport)
	if err != nil {
		return err
	}
	defer func() { _ = node.Shutdown().Error() }()
	if *bootstrap {
		if err = node.BootstrapCluster(raft.Configuration{Servers: servers}).Error(); err != nil {
			return err
		}
	}
	listener, err := net.Listen("tcp", *httpAddr)
	if err != nil {
		return err
	}
	defer listener.Close()
	server := &http.Server{Handler: &api{node: node, fsm: f, id: *id, token: token, adminAddr: *httpAddr}, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: 30 * time.Second}
	return server.Serve(listener)
}
func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
