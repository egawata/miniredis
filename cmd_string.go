// Commands from http://redis.io/commands#string

package miniredis

import (
	"github.com/bsm/redeo"
)

// commandsString handles all string value operations.
func commandsString(m *Miniredis, srv *redeo.Server) {
	srv.HandleFunc("SET", func(out *redeo.Responder, r *redeo.Request) error {
		if len(r.Args) < 2 {
			out.WriteErrorString("Usage error")
			return nil
		}
		if len(r.Args) > 2 {
			// EX/PX/NX/XX options.
			return errUnimplemented
		}
		key := r.Args[0]
		value := r.Args[1]
		m.Lock()
		defer m.Unlock()

		m.stringKeys[key] = value
		// a SET clears the expire
		delete(m.expire, key)
		out.WriteOK()
		return nil
	})

	srv.HandleFunc("GET", func(out *redeo.Responder, r *redeo.Request) error {
		key := r.Args[0]
		m.Lock()
		defer m.Unlock()
		value, ok := m.stringKeys[key]
		if !ok {
			out.WriteNil()
			return nil
		}
		out.WriteString(value)
		return nil
	})

	// TODO: GETSET (clears expire!)
}