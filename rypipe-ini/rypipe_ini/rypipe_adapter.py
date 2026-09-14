from typing import Any


class IniAdapter:
    """rypipe-compatible adapter for INI configuration files."""

    def read(self, path: str, **kwargs: Any) -> Any:
        """Parse ``path`` and return a ``pyarrow.Table``."""
        from rypipe_ini import _rypipe_ini

        return _rypipe_ini.read_ini(path, **kwargs)

    def iter_record_batches(
        self, path: str, memory: str | int = "64MiB",
        batch_size: int | None = None, **kwargs: Any,
    ):
        """Yield ``pyarrow.RecordBatch`` objects with constant memory."""
        yield from IniSource(path, **kwargs).iter_record_batches(
            memory=memory, batch_size=batch_size
        )


def _register() -> None:
    try:
        import rypipe
    except Exception:  # pragma: no cover
        return
    rypipe.register_adapter("ini", IniAdapter(), extensions=[".ini", ".cfg", ".conf"])


_register()
