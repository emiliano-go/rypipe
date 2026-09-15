from typing import Any


class IniAdapter:
    """rypipe-compatible adapter for INI configuration files."""

    def read(self, path: str, **kwargs: Any) -> Any:
        """Parse ``path`` and return a ``pyarrow.Table``."""
        from rypipe_ini import _rypipe_ini

        return _rypipe_ini.read_ini(path, **kwargs)



def _register() -> None:
    try:
        import rypipe
    except Exception:  # pragma: no cover
        return
    rypipe.register_adapter("ini", IniAdapter(), extensions=[".ini", ".cfg", ".conf"])


_register()
