# Security Policy

## Supported Versions

| Version | Supported          |
| ------- | ------------------ |
| 0.5.x   | :white_check_mark: |
| < 0.5   | :x:                |

## Reporting a Vulnerability

We take security vulnerabilities seriously. If you discover a security issue in CobaltDB, please report it responsibly.

**Do NOT open a public GitHub issue for security vulnerabilities.**

Instead, please:

1. Email **ersinkoc@gmail.com** with the subject `CobaltDB Security: <brief description>`
2. Include the following information:
   - Type of vulnerability (e.g., buffer overflow, SQL injection, authentication bypass)
   - Full steps to reproduce
   - Affected versions
   - Potential impact
   - Any suggested fixes (optional)

## Response Timeline

- **Acknowledgment**: Within 48 hours
- **Initial assessment**: Within 5 business days
- **Fix timeline**: Depends on severity
  - Critical: Patch release within 72 hours
  - High: Patch release within 1 week
  - Medium/Low: Next scheduled release

## Security Features

CobaltDB includes the following security features:

- TLS encryption for client connections
- Row-Level Security (RLS) with policy-based access control
- AES-256-GCM encryption at rest
- Audit logging for compliance
- SQL injection protection via prepared statements
- Argon2id password hashing

## Disclosure Policy

- We follow responsible disclosure
- We request 90 days before public disclosure of vulnerabilities
- We will credit researchers in our security advisories (unless anonymity is requested)
