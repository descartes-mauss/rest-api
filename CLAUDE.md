# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Run development server
uvicorn main:app --reload --host 0.0.0.0 --port 8000

# Run all tests
python -m pytest tests/*.py

# Run a single test
pytest tests/test_topics.py::test_list_topics

# Run tests with coverage
python -m pytest tests/*.py --cov-config=.coveragerc --cov-report html

# Quality checks (black → isort → ruff → mypy)
./qa.sh

# Install pre-commit hooks
pre-commit install
```

## Architecture

This is a **Python 3.12 FastAPI** project using a strict 3-layer architecture:

```
routes/ → services/ → repositories/ → database/
```

- **`routes/`**: Thin FastAPI routers. No business logic — only orchestration and HTTP concerns.
- **`services/`**: Business logic layer injected into routes via FastAPI `Depends`.
- **`repositories/`**: Data access layer. Uses Protocol-based abstractions for dependency injection and testability.
- **`database/`**: DB sessions, models, and schemas.
  - `public_models/`: SQLModel definitions for the public PostgreSQL schema.
  - `tenant_models/`: SQLModel definitions for per-tenant schemas.
  - `schemas/`: Pydantic DTO models for API responses.
  - `session.py`: `DBSession` provides `session()` (public schema) and `tenant_session(tenant_schema)` (sets `search_path`).
  - `manager.py`: Generic DB helpers (`get_all`, `get_by_id`, `get_topics`, etc.).

### Multi-tenancy

The system uses PostgreSQL schema-based multi-tenancy. The JWT payload's `orgId` field determines the tenant schema. `tenant_session()` sets `search_path` to isolate per-tenant data.

### Authentication

JWT validation is centralized in `jwt_validator.py`. All protected endpoints use `validate_jwt` as a FastAPI dependency. The JWT payload must include `orgId` for tenant-scoped operations.

### Testing pattern

Tests override FastAPI dependencies to inject fakes:

```python
app.dependency_overrides[validate_jwt] = lambda: {"orgId": "test_schema"}
app.dependency_overrides[get_topic_service] = lambda: TopicService(FakeRepo())
```

## Style & Tooling

- **Formatter**: `black` (line-length 100)
- **Linter**: `ruff` (auto-fix enabled)
- **Import sort**: `isort` (black profile)
- **Type checker**: `mypy --strict` (excludes `migrations/` and `tests/`)
- **Coverage**: minimum 80%, reports to `htmlcov/`

Pre-commit hooks run black, isort, ruff, mypy, and pytest on every commit (`fail_fast: true`).

## Key Conventions

- Use `async` endpoints unless a blocking dependency requires sync.
- Use `SQLModel` for DB models; use separate Pydantic models in `database/schemas/` for API responses.
- Routers must not implement business rules — extract to services.
- Prefer explicit transactions; close sessions promptly.
- Feature branches: `feature/<short-desc>`, bugfix branches: `fix/<short-desc>`.
- DB schema changes require an Alembic migration in `migrations/`.
