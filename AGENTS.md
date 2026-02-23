# AGENTS.md

This repository is a Python 3.12 FastAPI project. This document prescribes
engineering standards, tooling, and best practices to ensure high-quality,
PEP8-compliant, well-tested code.

**Audience:** contributors, reviewers, and maintainers.

**Goals:**

- Enforce PEP8 and modern Python idioms for 3.12.
- Ensure consistency via automated formatting and linting.
- Promote safe, well-typed FastAPI design and reliable tests.

______________________________________________________________________

## Stack

- Python 3.12
- FastAPI (ASGI)
- SQLModel / SQLAlchemy
- Uvicorn for development/production ASGI server
- httpx for external requests and testing
- PyTest for tests

### Important packages

- `fastapi`
- `sqlmodel`
- `uvicorn`
- `httpx`
- `pydantic`
- `python-jose`
- `alembic`
- `pytest`
- `ruff`
- `black`
- `isort`
- `mypy`.

______________________________________________________________________

## Repository & Structure\*\*

- Keep the existing layout; follow these rules when adding files:
  - `database/` holds DB sessions, models, and migrations helpers.
  - `routes/` holds API routers grouped by domain.
  - `public_models/` and `tenant_models/` contain SQLModel/Pydantic models.
  - Keep small modules and functions; prefer composition over large classes.

______________________________________________________________________

## Style & Formatting (required)

- Follow PEP8 strictly for Python code.
- Use `black` for formatting with line-length 88 (project default).
- Use `ruff` as the linter (recommended config to enforce PEP8 rules).
- Use `isort` for import sorting (integrated with `ruff` or configured via
  `pyproject.toml`).
- All changes must pass `ruff check` and `black --check` before merge.

Example `pyproject.toml` entries (canonicalized in project settings):

- `tool.black` — `line-length = 88`.
- `tool.ruff` — enable selected rules, fixable rules enabled.

______________________________________________________________________

## Typing & MyPy

- Use explicit, strong typing. Favor `typing` and `typing_extensions` where
  necessary. Avoid `Any` except very intentionally documented cases.
- Run `mypy` (or `ruff mypy` integration) in CI with strict settings for
  public modules. Use `--strict` for core packages when feasible.

## Pydantic / SQLModel

- Use `SQLModel` models for DB objects; use separate Pydantic models for
  request/response data/transfer schemas when needed.
- Keep models immutable where appropriate and validate at construction.

______________________________________________________________________

## FastAPI Best Practices\*\*

- Keep routers thin: routers should orchestrate application logic, not
  implement business rules — extract services/helpers.
- Dependency injection: use FastAPI dependencies for sessions, auth, and
  common parameters.
- Use async endpoints unless a blocking dependency requires sync.
- Validate request and response models explicitly using Pydantic/SQLModel.
- Return typed responses and document response models in route decorators.

## Security\*\*

- Centralize JWT/auth logic (see `jwt_validator.py`).
- Do not store secrets in source; use environment variables and loading via
  `.env` in development only.

______________________________________________________________________

## Database & Migrations\*\*

- Use Alembic for migrations. Keep migration files in a `migrations/` folder.
- Keep DB session creation in a single module (`database/session.py`).
- Prefer explicit transactions; close sessions promptly.

______________________________________________________________________

## Testing & CI\*\*

- Use `pytest` with `httpx.AsyncClient` for FastAPI integration tests.
- Keep tests fast and hermetic — mock external APIs using `respx` or `pytest-mock`.
- Aim for >80% coverage on business-critical modules; measure in CI.
- Run `ruff`, `black --check`, `mypy`, and `pytest` in CI (pre-merge).

Example test command:

```bash
pytest -q
```

______________________________________________________________________

## Pre-commit & Automation\*\*

- Install `pre-commit` and enable hooks for `ruff`, `black`, `isort`, and
  safety checks for secrets.
- Example hooks: format (`black`), lint (`ruff`), sort imports (`isort`),
  check types (`ruff`/`mypy`).

______________________________________________________________________

## Code Review Checklist\*\*

- Is the code PEP8-compliant and formatted by `black`?
- Are types present for public functions and interfaces?
- Are endpoints documented and response models specified?
- Are DB changes accompanied by migrations?
- Are new dependencies justified and minimal?
- Are tests added for new behavior and passing locally?

______________________________________________________________________

## Commit & Branching\*\*

- Use concise, imperative commit messages.
- Feature branches named `feature/<short-desc>`; bugfix `fix/<short-desc>`.
- Use PRs with at least one approving review and passing CI.

______________________________________________________________________
