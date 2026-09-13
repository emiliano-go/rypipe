class RenameFields:
    __slots__ = ("_mapping",)

    def __init__(self, mapping: dict[str, str]):
        # Detect target-name collisions: multiple sources → same target
        targets = list(mapping.values())
        seen: dict[str, str] = {}
        for src, tgt in mapping.items():
            if tgt in seen:
                raise ValueError(
                    f"RenameFields: source fields {seen[tgt]!r} and {src!r} "
                    f"both map to {tgt!r}; each target name must be unique"
                )
            seen[tgt] = src
        self._mapping = mapping

    def apply(self, record: dict) -> dict:
        mapping = self._mapping
        return {mapping.get(k, k): v for k, v in record.items()}

    def __call__(self, stream):
        return map(self.apply, stream)

    def _plan_kwargs(self) -> dict | None:
        return {"field_mapping": self._mapping}
