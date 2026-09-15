"""Exception classes shared by the engine and separately built adapters."""


class ParseError(Exception):
    """Malformed input, including invalid UTF-8."""


class XmlError(ParseError):
    """Compatibility name for XML parse failures."""


class PlanError(Exception):
    """Invalid execution plan."""


class MergeError(Exception):
    """Incompatible chunks during merge."""


class ParserError(Exception):
    """An adapter parser or worker failed."""
