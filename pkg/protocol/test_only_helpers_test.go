package protocol

// Test-only helpers. These have no production callers and live in a _test.go
// file so the lint gate's unused-code check stays clean.

func (s *MySQLServer) authEnabled() bool {
	authenticator, _ := s.authSnapshot()
	return authenticator != nil && authenticator.IsEnabled()
}
