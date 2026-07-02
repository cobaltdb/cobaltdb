package main

// Test-only helpers. These have no production callers and live in a _test.go
// file so the lint gate's unused-code check stays clean.

// setAPIToken installs a single bootstrap admin token, mirroring the legacy
// single-token configuration path.
func (s *Server) setAPIToken(token string) {
	if s.tokens == nil {
		s.tokens = newTokenStore()
	}
	s.tokens.setBootstrap(token)
}

// secureTokenCompare reports whether the raw token authenticates as any
// non-expired principal.
func (s *Server) secureTokenCompare(token string) bool {
	if s.tokens == nil {
		return false
	}
	_, ok := s.tokens.resolve(token)
	return ok
}
