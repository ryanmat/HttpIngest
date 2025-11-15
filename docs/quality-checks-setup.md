# Quality Checks Setup Guide

**Status:** To be implemented after feature development completes
**Purpose:** Set up pre-commit hooks with ruff and pytest before merging to main

## Overview

This document outlines the steps to add automated quality checks to the repository. These checks will run automatically before each commit to ensure code quality and test coverage.

## Prerequisites

- All feature development on `feature/production-redesign` complete
- All tests passing
- Ready to prepare for merge to `main`

## Setup Steps

### 1. Install Pre-commit Framework

```bash
# Add pre-commit to dev dependencies
uv add --dev pre-commit

# Install pre-commit hooks
uv run pre-commit install
```

### 2. Create Pre-commit Configuration

Create `.pre-commit-config.yaml` in project root:

```yaml
repos:
  # Ruff - Python linter and formatter
  - repo: https://github.com/astral-sh/ruff-pre-commit
    rev: v0.14.5
    hooks:
      # Run the linter
      - id: ruff
        args: [--fix]
      # Run the formatter
      - id: ruff-format

  # Run pytest before commit
  - repo: local
    hooks:
      - id: pytest
        name: pytest
        entry: uv run pytest
        language: system
        pass_filenames: false
        always_run: true
        stages: [commit]
```

### 3. Create Ruff Configuration (Optional)

Add to `pyproject.toml` if needed:

```toml
[tool.ruff]
line-length = 120
target-version = "py310"

[tool.ruff.lint]
select = [
    "E",   # pycodestyle errors
    "W",   # pycodestyle warnings
    "F",   # pyflakes
    "I",   # isort
    "B",   # flake8-bugbear
    "C4",  # flake8-comprehensions
    "UP",  # pyupgrade
]
ignore = [
    "E501",  # line too long (handled by formatter)
]

[tool.ruff.lint.per-file-ignores]
"__init__.py" = ["F401"]  # Allow unused imports in __init__.py
```

### 4. Test the Setup

```bash
# Test pre-commit on all files
uv run pre-commit run --all-files

# Test a single commit
git add <file>
git commit -m "test: verify pre-commit hooks"
```

### 5. Update CI/CD (Future - Optional)

If adding GitHub Actions later:

```yaml
# .github/workflows/test.yml
name: Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: '3.10'
      - name: Install uv
        run: curl -LsSf https://astral.sh/uv/install.sh | sh
      - name: Install dependencies
        run: uv sync
      - name: Run tests
        run: uv run pytest -v
      - name: Run ruff
        run: uv run ruff check .
```

## What Gets Checked

### Ruff
- Code formatting (auto-fixes)
- Import sorting
- Common Python bugs
- Code style violations
- Unused imports

### Pytest
- All unit tests must pass
- Integration tests must pass
- No test failures allowed

## Workflow

Once set up, the workflow becomes:

```bash
# Make changes
vim src/function_app.py

# Stage changes
git add src/function_app.py

# Commit (hooks run automatically)
git commit -m "feat: add new endpoint"
# → Ruff runs (auto-fixes code)
# → Pytest runs (all tests must pass)
# → If both pass, commit succeeds
# → If either fails, commit is blocked

# If auto-fixes were made, review and add them
git add src/function_app.py
git commit -m "feat: add new endpoint"

# Push when ready
git push origin feature/production-redesign
```

## Bypassing Hooks (Emergency Only)

**WARNING:** Only use in emergencies. Never bypass for main branch.

```bash
git commit --no-verify -m "emergency: critical hotfix"
```

## Notes

- Pre-commit hooks only run on staged files
- Hooks can auto-fix many issues (especially formatting)
- Tests must pass before any commit is allowed
- This ensures `feature/production-redesign` stays clean
- Makes merge to `main` much smoother

## Estimated Setup Time

- 5-10 minutes to configure
- 30-60 minutes for first full run (fixes any existing issues)
- ~10 seconds per commit thereafter

---

**Created:** 2025-11-14
**For:** feature/production-redesign branch quality assurance
