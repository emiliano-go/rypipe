from typing import Any

from rypipe import Source
from rypipe_ldif import _rypipe_ldif


class LdifSource(Source):
    """Pipeline-capable source for LDIF (LDAP Data Interchange Format) files."""

    def _read_arrow(self, plan_overrides: dict[str, Any] | None = None) -> Any:
        plan = self._build_plan_kwargs()
        if plan_overrides:
            plan.update(plan_overrides)
        return _rypipe_ldif.read_ldif(str(self._path), **plan)
