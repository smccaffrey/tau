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

**The API key is equivalent to code execution.** Anyone holding it can deploy a pipeline,
which is arbitrary Python that runs with the daemon's (or worker's) permissions and can
read its environment, including every secret available to `ctx.secret()`. There are no
per-user roles. Treat the key like a root password.

Before exposing a daemon beyond localhost:

- Set a long random `TAU_API_KEY` (`openssl rand -hex 32`). The daemon warns at startup if
  it listens on a non-loopback address with the default key.
- Put TLS in front of it (a reverse proxy or load balancer), and keep it off the public
  internet where you can. Workers only need to be reachable from coordinators.
- Run `taud` as an unprivileged user (the Docker image does) with only the credentials
  its pipelines need.
- Workers send their own API key to the coordinator when they register, and coordinators
  store it in the metadata database so they can send runs to the worker. Protect that
  database accordingly.
- Webhook secrets are stored in the metadata database and sent as `X-Tau-Secret`.
