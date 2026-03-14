# Contributing to TreeHub

Thank you for your interest in contributing to TreeHub! 🌳

## Ways to Contribute

### 🆕 Add a New Platform

The highest-impact contribution is adding a new platform index:

1. **Open an issue** with the `platform-request` label
2. Include the platform name and `llms.txt` URL
3. Follow the steps in [`indices/_template/README.md`](indices/_template/README.md)

### 🐛 Report Bugs

- Use the issue tracker to report bugs
- Include steps to reproduce, expected behavior, and actual behavior

### 💡 Suggest Features

- Open an issue with the `enhancement` label
- Describe the use case and proposed solution

### 🔧 Submit Code

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/my-feature`
3. Make your changes
4. Run validation: `python scripts/validator.py --all`
5. Submit a pull request

## Development Setup

```bash
# Clone the repo
git clone https://github.com/treehub/indices.git
cd treehub

# Create virtual environment
python -m venv .venv
source source .venv/bin/activate

# Install with dev dependencies
pip install -e ".[dev]"

# Run tests
pytest

# Run linter
ruff check .

# Run type checker
mypy scripts/ cli/ mcp-server/
```

## Code Style

- We use [Ruff](https://github.com/astral-sh/ruff) for linting and formatting
- Type hints are required for all public functions
- Docstrings follow Google style

## Pull Request Process

1. Ensure all validations pass (`python scripts/validator.py --all`)
2. Update documentation if you change behavior
3. Add tests for new features
4. Keep PRs focused — one feature/fix per PR

## Index Contribution Guidelines

When contributing a new platform index:

- **File naming:** `<version>-tree.json` and `<version>-manifest.json`
- **Schema compliance:** Must pass `python scripts/validator.py`
- **Integrity:** Include SHA-256 hashes in manifest
- **Registry:** Add entry to `registry.json`

## Schema Changes

- Schema versions follow SemVer
- Breaking changes require a major version bump
- Minimum 30-day migration period for breaking changes

## Code of Conduct

Be kind, constructive, and respectful. We're all here to build something useful.

## Questions?

Open an issue or start a discussion — we're happy to help!
