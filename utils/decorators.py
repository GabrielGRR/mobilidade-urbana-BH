import time
from functools import wraps
from typing import Callable, Any
from utils.logging_utils import get_logger

logger = get_logger(__name__)

def retry(max_attempts: int = 3, delay: int = 2) -> Callable:
    """Decorator genérico para retentar execuções em caso de falha (ex: instabilidades de rede)."""
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            last_exception = None
            for tentativa in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    logger.warning(f"Tentativa {tentativa}/{max_attempts} falhou em {func.__name__}: {e}")
                    if tentativa < max_attempts:
                        time.sleep(delay)
            logger.error(f"Todas as {max_attempts} tentativas falharam na função {func.__name__}.")
            raise last_exception
        return wrapper
    return decorator
