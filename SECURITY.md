# Security policy

## Reporting a vulnerability

Please report vulnerabilities privately through
[GitHub security advisories](https://github.com/smccaffrey/tau/security/advisories/new)
rather than in a public issue. Include the affected version, the steps to
reproduce, and the impact you expect.

You should get an acknowledgement within 3 business days. We'll keep you updated
while we work on a fix, and credit you in the release notes unless you'd rather
stay anonymous.

## Supported versions

Security fixes go into the latest release. Please upgrade before reporting.

## Hardening your deployment

Before exposing a daemon beyond localhost:
set a random `TAU_API_KEY`, put TLS in front of it, and keep it off the public
internet where you can. Pipeline code runs with the daemon's permissions.
