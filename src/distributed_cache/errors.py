class CacheError(Exception):
    """Stable, machine-readable cache or transport failure."""

    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code
