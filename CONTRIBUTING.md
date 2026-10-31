# Contributing to Tau

Thanks for helping. Issues, ideas, docs fixes and pull requests are all welcome.

- **Bugs:** open an issue with steps to reproduce, the output of `tau version`, and the relevant `tau inspect` or `tau logs` output.
- **Features:** open an issue first for anything larger than a small fix, so we can agree on the approach.
- **Security issues:** see [SECURITY.md](SECURITY.md). Please don't file a public issue.

## Setup

```bash
uv sync --all-extras
uv run pytest
uv run ruff check src tests
```

## Pull requests

1. Branch from `main` and keep each PR focused on one change.
2. Add tests. Connectors and materializations should cover every dialect they touch.
3. Make sure `uv run pytest` and `uv run ruff check src tests` pass. CI runs both.
4. Update `README.md` and `CLAUDE.md` when you change the CLI, the pipeline API or configuration.

Adding a connector? Subclass the ABC in `src/tau/connectors/base.py`, implement
`extract`, `load`, `execute` and `get_schema`, and register an optional extra in
`pyproject.toml`.

This project follows the [Contributor Covenant](CODE_OF_CONDUCT.md). By
contributing, you agree that your contributions are licensed under the [MIT License](LICENSE).
