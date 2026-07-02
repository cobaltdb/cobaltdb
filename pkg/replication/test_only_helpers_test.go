package replication

// Test-only helpers. These wrappers have no production callers and live in a
// _test.go file so the lint gate's unused-code check stays clean.

// prepareSlaveResume prepares a slave connection to resume from the requested
// LSN, using the same path as the RESUME handshake.
func (m *Manager) prepareSlaveResume(slave *SlaveConnection, requestedLSN uint64) error {
	return m.prepareSlaveResumeRequest(slave, resumeRequest{LSN: requestedLSN})
}
