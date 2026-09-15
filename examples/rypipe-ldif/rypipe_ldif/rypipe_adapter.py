from typing import Any


class LdifAdapter:
    """rypipe-compatible adapter for LDIF files."""

    def read(self, path: str, **kwargs: Any) -> Any:
        """Parse ``path`` and return a ``pyarrow.Table``."""
        from rypipe_ldif import _rypipe_ldif

        return _rypipe_ldif.read_ldif(path, **kwargs)



def _register() -> None:
    try:
        import rypipe
    except Exception:  # pragma: no cover
        return
    rypipe.register_adapter("ldif", LdifAdapter(), extensions=[".ldif", ".ldf"])


_register()
