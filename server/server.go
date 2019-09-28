package server

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"sync"
	"unicode"
)

func errUnknownCommand(cmd string, args []string) string {
	s := fmt.Sprintf("ERR unknown command `%s`, with args beginning with: ", cmd)
	if len(args) > 20 {
		args = args[:20]
	}
	for _, a := range args {
		s += fmt.Sprintf("`%s`, ", a)
	}
	return s
}

// Cmd is what Register expects
type Cmd func(c *Peer, cmd string, args []string)

type DisconnectHandler func(c *Peer)

// Server is a simple redis server
type Server struct {
	l         net.Listener
	cmds      map[string]Cmd
	peers     map[net.Conn]struct{}
	mu        sync.Mutex
	wg        sync.WaitGroup
	infoConns int
	infoCmds  int
}

// NewServer makes a server listening on addr. Close with .Close().
func NewServer(addr string) (*Server, error) {
	s := Server{
		cmds:  map[string]Cmd{},
		peers: map[net.Conn]struct{}{},
	}

	l, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	s.l = l

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		// l.Accept() が失敗するとこの goroutine が終了する。
		// Close 時に l(net.Listner) が close され、その後 Accept() は失敗するはずなので
		// それで抜ける、ということらしい。
		s.serve(l)
	}()
	return &s, nil
}

func (s *Server) serve(l net.Listener) {
	for {
		conn, err := l.Accept()
		if err != nil {
			return
		}
		s.ServeConn(conn)

		// もしかしたら、ここで s.closed みたいなフラグを見て
		// true ならこのループを抜ける、という処理のほうが優しいのかもしれない
		// ただ、優しさが求められるのは conn だけであって、listener は
		// この方法でも十分なのかもしれない。
	}
}

// ServeConn handles a net.Conn. Nice with net.Pipe()
func (s *Server) ServeConn(conn net.Conn) {
	// wg.Add(1) -> goroutine 開始 -> その中ではじめに defer wg.Done() が定石
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		// conn.Close() も忘れないこと
		defer conn.Close()
		s.mu.Lock()
		s.peers[conn] = struct{}{}
		s.infoConns++
		s.mu.Unlock()

		s.servePeer(conn)

		s.mu.Lock()
		delete(s.peers, conn)
		s.mu.Unlock()
	}()
}

// Addr has the net.Addr struct
func (s *Server) Addr() *net.TCPAddr {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.l == nil {
		return nil
	}
	return s.l.Addr().(*net.TCPAddr)
}

// Close a server started with NewServer. It will wait until all clients are
// closed.
func (s *Server) Close() {
	s.mu.Lock()
	if s.l != nil {
		s.l.Close()
	}
	s.l = nil
	// s.peers の key は net.Conn
	for c := range s.peers {
		c.Close()
	}
	s.mu.Unlock()

	// listener, conn ごとに s.wg.Add(1)している
	// これらの処理がすべて終了するまで待つ
	s.wg.Wait()
}

// Register a command. It can't have been registered before. Safe to call on a
// running server.
// ある cmd に対して、Server 側がどう処理すべきかを呼び出し側から渡せる。
func (s *Server) Register(cmd string, f Cmd) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cmd = strings.ToUpper(cmd)
	if _, ok := s.cmds[cmd]; ok {
		return fmt.Errorf("command already registered: %s", cmd)
	}
	s.cmds[cmd] = f
	return nil
}

func (s *Server) servePeer(c net.Conn) {
	r := bufio.NewReader(c)
	peer := &Peer{
		w: bufio.NewWriter(c),
	}
	// peer.onDisconnect に、終了時に実行したい function を登録しておくと
	// すべて実行される
	defer func() {
		for _, f := range peer.onDisconnect {
			f()
		}
	}()

	for {
		// 終端まで読み込んだ場合は err = io.EOF となる。
		// この場合は処理を終了
		args, err := readArray(r)
		if err != nil {
			return
		}
		s.dispatch(peer, args)
		peer.Flush()
		s.mu.Lock()
		closed := peer.closed
		s.mu.Unlock()
		if closed {
			c.Close()
		}
	}
}

func (s *Server) dispatch(c *Peer, args []string) {
	cmd, args := args[0], args[1:]
	cmdUp := strings.ToUpper(cmd)
	// struct 上の、変更可能性のある field にアクセスする場合は
	// 前後で Lock をかける
	s.mu.Lock()
	cb, ok := s.cmds[cmdUp]
	s.mu.Unlock()
	if !ok {
		c.WriteError(errUnknownCommand(cmd, args))
		return
	}

	s.mu.Lock()
	s.infoCmds++
	s.mu.Unlock()
	cb(c, cmdUp, args)
}

// TotalCommands is total (known) commands since this the server started
func (s *Server) TotalCommands() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.infoCmds
}

// ClientsLen gives the number of connected clients right now
func (s *Server) ClientsLen() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.peers)
}

// TotalConnections give the number of clients connected since the server
// started, including the currently connected ones
func (s *Server) TotalConnections() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.infoConns
}

// Peer is a client connected to the server
type Peer struct {
	w            *bufio.Writer
	closed       bool
	Ctx          interface{} // anything goes, server won't touch this
	onDisconnect []func()    // list of callbacks
	mu           sync.Mutex  // for Block()
}

// Flush the write buffer. Called automatically after every redis command
// bufio.Writer は出力結果を一時的にためて、Flush で io.Writer に書き出す。
// 適当なタイミングでFlush()する必要がある。
func (c *Peer) Flush() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.w.Flush()
}

// Close the client connection after the current command is done.
// Close()が呼ばれた時点で Close してしまうのではなく、とりあえず
// フラグだけ立てておく。
// conn を処理中の for loop 内で、リクエスト処理完了ごとにこのフラグの値を調べ、
// true だったら実際に conn.Close() する
// これにより、リクエスト処理中にいきなりコネクションを切ることがなくなる。
func (c *Peer) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
}

// Register a function to execute on disconnect. There can be multiple
// functions registered.
func (c *Peer) OnDisconnect(f func()) {
	c.onDisconnect = append(c.onDisconnect, f)
}

// issue multiple calls, guarded with a mutex
func (c *Peer) Block(f func(*Writer)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	f(&Writer{c.w})
}

// WriteError writes a redis 'Error'
func (c *Peer) WriteError(e string) {
	// c.w にアクセスするので、出力の前後に Lock をかけなければならない
	// 冗長になるので、Lock をかける部分を Block にまかせ、
	// func をまるごと渡すという手法が使える
	c.Block(func(w *Writer) {
		w.WriteError(e)
	})
}

// WriteInline writes a redis inline string
func (c *Peer) WriteInline(s string) {
	c.Block(func(w *Writer) {
		w.WriteInline(s)
	})
}

// WriteOK write the inline string `OK`
func (c *Peer) WriteOK() {
	c.WriteInline("OK")
}

// WriteBulk writes a bulk string
func (c *Peer) WriteBulk(s string) {
	c.Block(func(w *Writer) {
		w.WriteBulk(s)
	})
}

// WriteNull writes a redis Null element
func (c *Peer) WriteNull() {
	c.Block(func(w *Writer) {
		w.WriteNull()
	})
}

// WriteLen starts an array with the given length
func (c *Peer) WriteLen(n int) {
	c.Block(func(w *Writer) {
		w.WriteLen(n)
	})
}

// WriteInt writes an integer
func (c *Peer) WriteInt(i int) {
	c.Block(func(w *Writer) {
		w.WriteInt(i)
	})
}

func toInline(s string) string {
	return strings.Map(func(r rune) rune {
		if unicode.IsSpace(r) {
			return ' '
		}
		return r
	}, s)
}

// A Writer is given to the callback in Block()
type Writer struct {
	w *bufio.Writer
}

// WriteError writes a redis 'Error'
func (w *Writer) WriteError(e string) {
	fmt.Fprintf(w.w, "-%s\r\n", toInline(e))
}

func (w *Writer) WriteLen(n int) {
	fmt.Fprintf(w.w, "*%d\r\n", n)
}

// WriteBulk writes a bulk string
func (w *Writer) WriteBulk(s string) {
	fmt.Fprintf(w.w, "$%d\r\n%s\r\n", len(s), s)
}

// WriteInt writes an integer
func (w *Writer) WriteInt(i int) {
	fmt.Fprintf(w.w, ":%d\r\n", i)
}

// WriteNull writes a redis Null element
func (w *Writer) WriteNull() {
	fmt.Fprintf(w.w, "$-1\r\n")
}

// WriteInline writes a redis inline string
func (w *Writer) WriteInline(s string) {
	fmt.Fprintf(w.w, "+%s\r\n", toInline(s))
}

func (w *Writer) Flush() {
	w.w.Flush()
}
