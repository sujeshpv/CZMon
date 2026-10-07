from typing import Optional, Mapping, Any


class CZMonError(Exception):
    """Base exception for the application/library."""
    def __init__(
        self,
        message: str,
        cause: Optional[BaseException] = None,
        context: Optional[Mapping[str, Any]] = None,
    ):
        super().__init__(message)
        self.message = message
        self.cause = cause
        self.context = context or {}
    def __str__(self) -> str:
        ctx = ""
        if self.context:
            ctx = " | context=" + ", ".join(
                f"{k}={v!r}" for k, v in self.context.items()
            )
        if self.cause:
            return (
                f"{self.message} | cause="
                f"{type(self.cause).__name__}: {self.cause}{ctx}"
            )
        return f"{self.message}{ctx}"
