from typing import Any

from rypipe import Adapter
from rypipe_ldif import _rypipe_ldif


class LdifSource(Adapter):
    """Read plain LDIF records; folded, base64, and URL values raise ParserError."""

    def read(self, path: str, **kwargs: Any) -> Any:
        return _rypipe_ldif.read_ldif(path, **kwargs)
