package audit

import (
	"bufio"
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

const maxAuditLogLineSize = 16 * 1024 * 1024

// VerificationResult summarizes a successful audit log verification.
type VerificationResult struct {
	Entries          int
	EncryptedEntries int
	LastHash         string
}

// VerifyLogFile verifies the hash chain in a JSON audit log file.
//
// Encrypted entries produced with Config.EncryptionKey are supported when the
// same key is provided. Text-format audit logs are intentionally rejected
// because their line format is not a stable canonical payload.
func VerifyLogFile(path string, encryptionKey []byte) (*VerificationResult, error) {
	// #nosec G304 -- Audit verification intentionally opens caller-supplied log paths.
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open audit log: %w", err)
	}
	defer file.Close()

	var aead cipher.AEAD
	if len(encryptionKey) > 0 {
		aead, err = auditLogAEAD(encryptionKey)
		if err != nil {
			return nil, err
		}
	}

	result := &VerificationResult{}
	expectedPrevHash := ""
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 64*1024), maxAuditLogLineSize)
	lineNo := 0
	for scanner.Scan() {
		lineNo++
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}

		if bytes.HasPrefix(line, []byte("ENC:")) {
			if aead == nil {
				return nil, fmt.Errorf("audit log line %d: encrypted entry requires encryption key", lineNo)
			}
			line, err = decryptAuditLogLine(aead, line)
			if err != nil {
				return nil, fmt.Errorf("audit log line %d: decrypt entry: %w", lineNo, err)
			}
			result.EncryptedEntries++
		}

		if !bytes.HasPrefix(line, []byte("{")) {
			return nil, fmt.Errorf("audit log line %d: only JSON audit logs can be verified", lineNo)
		}

		var event Event
		if err := json.Unmarshal(line, &event); err != nil {
			return nil, fmt.Errorf("audit log line %d: decode event: %w", lineNo, err)
		}
		if event.Hash == "" {
			return nil, fmt.Errorf("audit log line %d: missing hash", lineNo)
		}
		if event.PrevHash != expectedPrevHash {
			return nil, fmt.Errorf("audit log line %d: previous hash mismatch", lineNo)
		}

		actualHash := event.Hash
		event.Hash = ""
		payload, err := json.Marshal(&event)
		if err != nil {
			return nil, fmt.Errorf("audit log line %d: canonicalize event: %w", lineNo, err)
		}
		expectedHash := hashAuditPayload(event.PrevHash, payload)
		if actualHash != expectedHash {
			return nil, fmt.Errorf("audit log line %d: hash mismatch", lineNo)
		}

		result.Entries++
		result.LastHash = actualHash
		expectedPrevHash = actualHash
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read audit log: %w", err)
	}

	return result, nil
}

func auditLogAEAD(key []byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("audit log encryption setup failed: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("audit log GCM setup failed: %w", err)
	}
	return gcm, nil
}

func decryptAuditLogLine(aead cipher.AEAD, line []byte) ([]byte, error) {
	encoded := strings.TrimSpace(string(bytes.TrimPrefix(line, []byte("ENC:"))))
	encrypted, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, fmt.Errorf("decode ciphertext: %w", err)
	}
	nonceSize := aead.NonceSize()
	if len(encrypted) < nonceSize {
		return nil, fmt.Errorf("ciphertext shorter than nonce")
	}
	nonce := encrypted[:nonceSize]
	ciphertext := encrypted[nonceSize:]
	plain, err := aead.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, err
	}
	return bytes.TrimSpace(plain), nil
}
