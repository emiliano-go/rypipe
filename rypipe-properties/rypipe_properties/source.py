from typing import Any

from rypipe import Source
from rypipe_properties import _rypipe_properties


class PropertiesSource(Source):
    """Pipeline-capable source for Java .properties files."""

    def _read_arrow(self, plan_overrides: dict[str, Any] | None = None) -> Any:
        plan = self._build_plan_kwargs()
        if plan_overrides:
            plan.update(plan_overrides)
        return _rypipe_properties.read_properties(str(self._path), **plan)
