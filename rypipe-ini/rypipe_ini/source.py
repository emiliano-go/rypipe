from typing import Any

from rypipe import Source
from rypipe_ini import _rypipe_ini


class IniSource(Source):
    """Pipeline-capable source for INI configuration files."""

    def _read_arrow(self, plan_overrides: dict[str, Any] | None = None) -> Any:
        plan = self._build_plan_kwargs()
        if plan_overrides:
            plan.update(plan_overrides)
        return _rypipe_ini.read_ini(str(self._path), **plan)
