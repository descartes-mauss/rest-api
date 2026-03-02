from typing import Any, Protocol


class DBSessionProvider(Protocol):
    """Protocol for a DB provider that exposes both public and tenant-scoped sessions."""

    def session(self) -> Any: ...

    def tenant_session(self, schema: str) -> Any: ...


__all__ = ["DBSessionProvider"]
