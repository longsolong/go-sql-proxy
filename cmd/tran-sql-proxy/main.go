package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/siddontang/go-mysql/client"
	"github.com/siddontang/go-mysql/packet"
	"github.com/siddontang/go-mysql/server"
	"github.com/sirupsen/logrus"
)

var (
	listenUseDomainSocket  = flag.Bool("listen-use-domain-socket", false, "listen on a domain socket instead of a TCP port")
	listen                 = flag.String("listen", ":5123", "listening addr")
	listenDomainSocket     = flag.String("listen-domain-socket", "/tmp/invalid.sock", "listening domain sock")
	backendUseDomainSocket = flag.Bool("backend-use-domain-socket", false, "backend use a domain socket instead of a TCP port")
	backend                = flag.String("backend", "localhost:3306", "backend addr")
	backendDomainSocket    = flag.String("backend-domain-socket", "/tmp/mysql.sock", "backend domain sock")
	backendUsername        = flag.String("backend-username", "root", "backend username")
	backendPassword        = flag.String("backend-password", "", "backend password")
	backendDbName          = flag.String("backend-dbname", "test", "backend dbname")
	dialTimeout            = flag.Duration("dial-timeout", 3*time.Second, "dial timeout")
)

func main() {
	flag.Parse()

	p := &Proxy{}

	logrus.Fatalf("%s", p.Run())
}

// Proxy routes connections to backends based on a Config.
type Proxy struct {
	l net.Listener

	donec chan struct{} // closed before err
	err   error         // any error from listening
}

// Run is calls Start, and then Wait.
//
// It blocks until there's an error. The return value is always
// non-nil.
func (p *Proxy) Run() error {
	if err := p.Start(); err != nil {
		return err
	}
	logrus.Printf("proxy start, listen on %q", *listen)
	return p.Wait()
}

// Wait waits for the Proxy to finish running. Currently this can only
// happen if a Listener is closed, or Close is called on the proxy.
//
// It is only valid to call Wait after a successful call to Start.
func (p *Proxy) Wait() error {
	<-p.donec
	return p.err
}

// Close closes all the proxy's self-opened listeners.
func (p *Proxy) Close() error {
	return p.l.Close()
}

// Start creates a TCP listener and starts the proxy. It returns any
// error from starting listeners.
//
// If it returns a non-nil error, any successfully opened listeners
// are closed.
func (p *Proxy) Start() error {
	if p.donec != nil {
		return errors.New("already started")
	}
	p.donec = make(chan struct{})
	errc := make(chan error, 1)
	var err error
	p.l, err = net.Listen("tcp", *listen)
	if err != nil {
		p.Close()
		return fmt.Errorf("create listener: %s", err)
	}
	go p.serveListener(errc, p.l)
	go p.awaitFirstError(errc)
	return nil
}

func (p *Proxy) awaitFirstError(errc <-chan error) {
	p.err = <-errc
	close(p.donec)
}

// Serve accepts connections from l and routes them.
func (p *Proxy) serveListener(errc chan<- error, ln net.Listener) {
	for {
		c, err := ln.Accept()
		if err != nil {
			errc <- err
			return
		}
		go func() {
			(&DialProxy{
				Addr:        *backend,
				Username:    *backendUsername,
				Password:    *backendPassword,
				DbName:      *backendDbName,
				DialTimeout: *dialTimeout,
			}).HandleConn(c)
		}()
	}
}

// DialProxy implements Target by dialing a new connection to Addr
// and then proxying data back and forth.
//
// The To func is a shorthand way of creating a DialProxy.
type DialProxy struct {
	Addr     string
	Username string
	Password string
	DbName   string

	// DialTimeout optionally specifies a dial timeout.
	// If zero, a default is used.
	// If negative, the timeout is disabled.
	DialTimeout time.Duration
}

// UnderlyingConn returns c.Conn if c of type *Conn,
// otherwise it returns c.
func UnderlyingConn(c net.Conn) net.Conn {
	if wrap, ok := c.(*packet.Conn); ok {
		return wrap.Conn
	}
	return c
}

// HandleConn implements the Target interface.
func (dp *DialProxy) HandleConn(src net.Conn) {
	serverConn, err := server.NewConn(src, dp.Username, dp.Password, server.EmptyHandler{})
	if err != nil {
		src.Close()
		logrus.Printf("for incoming conn %v, error handshake: %v", src.RemoteAddr().String(), err)
		return
	}
	defer serverConn.Close()

	clientConn, err := client.Connect(dp.Addr, dp.Username, dp.Password, dp.DbName)
	if err != nil {
		logrus.Printf("for incoming conn %v, error connect backend: %v", src.RemoteAddr().String(), err)
		return
	}
	defer clientConn.Close()

	logrus.Printf("%q <> %q: dialing success", src.RemoteAddr().String(), dp.Addr)
	errc := make(chan error, 1)
	go proxyCopy(errc, serverConn.Conn, clientConn.Conn)
	go proxyCopy(errc, clientConn.Conn, serverConn.Conn)
	<-errc
}

// proxyCopy is the function that copies bytes around.
// It's a named function instead of a func literal so users get
// named goroutines in debug goroutine stack dumps.
func proxyCopy(errc chan<- error, dst, src net.Conn) {
	// Unwrap the src and dst from *Conn to *net.TCPConn so Go
	// 1.11's splice optimization kicks in.
	src = UnderlyingConn(src)
	dst = UnderlyingConn(dst)

	_, err := io.Copy(dst, src)
	errc <- err
}

func (dp *DialProxy) dialTimeout() time.Duration {
	if dp.DialTimeout > 0 {
		return dp.DialTimeout
	}
	return 10 * time.Second
}
