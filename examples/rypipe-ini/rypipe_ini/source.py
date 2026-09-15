from typing import Any

from rypipe import Adapter
from rypipe_ini import _rypipe_ini


class IniSource(Adapter):
    """Pipeline-capable source for INI configuration files."""

    def read(self, path: str, **kwargs: Any) -> Any:
        return _rypipe_ini.read_ini(path, **kwargs)
