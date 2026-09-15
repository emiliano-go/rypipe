from typing import Any


class PropertiesAdapter:
    """rypipe-compatible adapter for Java .properties files."""

    def read(self, path: str, **kwargs: Any) -> Any:
        """Parse ``path`` and return a ``pyarrow.Table``."""
        from rypipe_properties import _rypipe_properties

        return _rypipe_properties.read_properties(path, **kwargs)



def _register() -> None:
    try:
        import rypipe
    except Exception:  # pragma: no cover
        return
    rypipe.register_adapter("properties", PropertiesAdapter(), extensions=[".properties"])


_register()
