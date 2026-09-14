from typing import Any


class LdifAdapter:
    """rypipe-compatible adapter for LDIF files."""

    def read(self, path: str, **kwargs: Any) -> Any:
        """Parse ``path`` and return a ``pyarrow.Table``."""
        from rypipe_ldif import _rypipe_ldif

        return _rypipe_ldif.read_ldif(path, **kwargs)

    def iter_record_batches(
        self, path: str, memory: str | int = "64MiB",
        batch_size: int | None = None, **kwargs: Any,
    ):
        """Yield ``pyarrow.RecordBatch`` objects with constant memory."""
        yield from LdifSource(path, **kwargs).iter_record_batches(
            memory=memory, batch_size=batch_size
        )


def _register() -> None:
    try:
        import rypipe
    except Exception:  # pragma: no cover
        return
    rypipe.register_adapter("ldif", LdifAdapter(), extensions=[".ldif", ".ldf"])


_register()
