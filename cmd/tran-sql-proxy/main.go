package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"time"
	"strings"

	"github.com/siddontang/go-mysql/client"
	"github.com/siddontang/go-mysql/packet"
	"github.com/siddontang/go-mysql/server"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/sirupsen/logrus"
)

var (
	listen                 = flag.String("listen", ":5123", "listening addr")
	backend                = flag.String("backend", "localhost:3306", "backend addr")
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
	p.l, err = net.Listen(getNetProto(*listen), *listen)
	if err != nil {
		p.Close()
		return fmt.Errorf("create listener: %s", err)
	}
	go p.serveListener(errc, p.l)
	go p.awaitFirstError(errc)
	return nil
}

func getNetProto(addr string) string {
	proto := "tcp"
	if strings.Contains(addr, "/") {
		proto = "unix"
	}
	return proto
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

func (dp *DialProxy) dialTimeout() time.Duration {
	if dp.DialTimeout > 0 {
		return dp.DialTimeout
	}
	return 10 * time.Second
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
	defer func() {
		if !serverConn.Closed() {
			serverConn.Close()
		}
	}()

	clientConn, err := client.Connect(dp.Addr, dp.Username, dp.Password, dp.DbName)
	if err != nil {
		logrus.Printf("for incoming conn %v, error connect backend: %v", src.RemoteAddr().String(), err)
		return
	}
	defer clientConn.Close()

	logrus.Printf("%q <> %q: proxy dialing success", src.RemoteAddr().String(), dp.Addr)
	errc := make(chan error, 1)
	go proxyCopy(errc, serverConn.Conn, clientConn.Conn)
	go func() {
		serverConn.Conn.Reader = io.TeeReader(serverConn.Conn.Reader, clientConn.Conn)
		for !serverConn.Closed() {
			HandleCommand(errc, serverConn)
		}
	}()

	err = <-errc
	logrus.Printf("%q <> %q: proxy exit err: %v", src.RemoteAddr().String(), dp.Addr, err)
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

func HandleCommand(errc chan<- error, serverConn *server.Conn) {
	data, err := serverConn.ReadPacket()
	if err != nil {
		errc <- err
		return
	}
	go dispatch(serverConn, data)

	if serverConn.Conn != nil {
		serverConn.ResetSequence()
	}
}

func dispatch(c *server.Conn, data []byte) {
	cmd := data[0]
	data = data[1:]

	switch cmd {
	case mysql.COM_QUIT:
		c.Close()
		logrus.Printf("%q COM_QUIT %s", c.RemoteAddr().String(), data)
	case mysql.COM_QUERY:
		logrus.Printf("%q COM_QUERY %s", c.RemoteAddr().String(), data)
	case mysql.COM_PING:
		logrus.Printf("%q COM_PING", c.RemoteAddr().String())
	case mysql.COM_INIT_DB:
		logrus.Printf("%q COM_INIT_DB %s", c.RemoteAddr().String(), data)
	case mysql.COM_FIELD_LIST:
		logrus.Printf("%q COM_FIELD_LIST", c.RemoteAddr().String())
	case mysql.COM_STMT_PREPARE:
		logrus.Printf("%q COM_STMT_PREPARE %s", c.RemoteAddr().String(), data)
	case mysql.COM_STMT_EXECUTE:
		logrus.Printf("%q COM_STMT_EXECUTE %s", c.RemoteAddr().String(), data)
	case mysql.COM_STMT_CLOSE:
		logrus.Printf("%q COM_STMT_CLOSE", c.RemoteAddr().String())
	case mysql.COM_STMT_SEND_LONG_DATA:
		logrus.Printf("%q COM_STMT_SEND_LONG_DATA %s", c.RemoteAddr().String(), data)
	case mysql.COM_STMT_RESET:
		logrus.Printf("%q COM_STMT_RESET %s", c.RemoteAddr().String(), data)
	default:
		logrus.Printf("%q CMD_OTHER %s", c.RemoteAddr().String(), data)
	}
}
