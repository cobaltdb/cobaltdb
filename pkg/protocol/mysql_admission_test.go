package protocol

import (
	"bufio"
	"errors"
	"testing"
)

type rejectingAdmission struct{}

func (rejectingAdmission) Admit(bool) error { return errors.New("overloaded") }

func TestMySQLAdmissionRejectsWorkCommandsWith1040(t *testing.T) {
	conn := newMockConn()
	conn.setReadData(commandPacket(MySQLComStatistics, nil))
	server := NewMySQLServer(nil, "test")
	server.SetAdmissionController(rejectingAdmission{})
	client := &MySQLClient{conn: conn, reader: bufio.NewReader(conn), server: server}
	if err := client.handleCommand(); err != nil {
		t.Fatalf("handleCommand: %v", err)
	}
	packet := conn.writeBuf.Bytes()
	if len(packet) < 7 || packet[4] != 0xff {
		t.Fatalf("response is not MySQL error packet: %x", packet)
	}
	code := uint16(packet[5]) | uint16(packet[6])<<8
	if code != 1040 {
		t.Fatalf("MySQL error code = %d, want 1040", code)
	}
}

func TestMySQLAdmissionKeepsPingCritical(t *testing.T) {
	conn := newMockConn()
	conn.setReadData(commandPacket(MySQLComPing, nil))
	server := NewMySQLServer(nil, "test")
	server.SetAdmissionController(rejectingAdmission{})
	client := &MySQLClient{conn: conn, reader: bufio.NewReader(conn), server: server}
	if err := client.handleCommand(); err != nil {
		t.Fatalf("handleCommand: %v", err)
	}
	packet := conn.writeBuf.Bytes()
	if len(packet) < 5 || packet[4] != 0x00 {
		t.Fatalf("ping response is not OK packet: %x", packet)
	}
}
