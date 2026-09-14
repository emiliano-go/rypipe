from typing import Any


class PropertiesAdapter:
    """rypipe-compatible adapter for Java .properties files."""

    def read(self, path: str, **kwargs: Any) -> Any:
        """Parse ``path`` and return a ``pyarrow.Table``."""
        from rypipe_properties import _rypipe_properties

        return _rypipe_properties.read_properties(path, **kwargs)

    def iter_record_batches(
        self, path: str, memory: str | int = "64MiB",
        batch_size: int | None = None, **kwargs: Any,
    ):
        """Yield ``pyarrow.RecordBatch`` objects with constant memory."""
        yield from PropertiesSource(path, **kwargs).iter_record_batches(
            memory=memory, batch_size=batch_size
        )


def _register() -> None:
    try:
        import rypipe
    except Exception:  # pragma: no cover
        return
    rypipe.register_adapter("properties", PropertiesAdapter(), extensions=[".properties"])


_register()
