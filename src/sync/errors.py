"""同步子系统复用全局领域错误，单独保留入口便于按子系统捕获。"""
from ..domain import Conflict, DomainError, NotFound, PermissionDenied, ValidationError

__all__ = ["DomainError", "ValidationError", "NotFound", "Conflict", "PermissionDenied"]
