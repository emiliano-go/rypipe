from typing import Any

from rypipe import Adapter
from rypipe_properties import _rypipe_properties


class PropertiesSource(Adapter):
    """Read plain properties records; Java escapes and continuations raise ParserError."""

    def read(self, path: str, **kwargs: Any) -> Any:
        return _rypipe_properties.read_properties(path, **kwargs)
