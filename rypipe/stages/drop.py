class DropFields:
    __slots__ = ("_fields_set",)

    def __init__(self, fields: list[str]):
        if isinstance(fields, str):
            raise TypeError(
                "DropFields expects a list of field names, got a bare "
                f"string; use DropFields([{fields!r}])"
            )
        self._fields_set = frozenset(fields)

    def apply(self, record: dict) -> dict:
        fields_set = self._fields_set
        if not fields_set:
            return record
        return {k: v for k, v in record.items() if k not in fields_set}

    def __call__(self, stream):
        return map(self.apply, stream)

    def _plan_kwargs(self) -> dict | None:
        return {"drop_fields": sorted(self._fields_set)}
