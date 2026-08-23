class RenameFields:
    __slots__ = ("_mapping",)

    def __init__(self, mapping: dict[str, str]):
        self._mapping = mapping

    def apply(self, record: dict) -> dict:
        mapping = self._mapping
        return {mapping.get(k, k): v for k, v in record.items()}

    def __call__(self, stream):
        return map(self.apply, stream)

    def _plan_kwargs(self) -> dict | None:
        return {"field_mapping": self._mapping}
