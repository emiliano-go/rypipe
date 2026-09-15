from rypipe import register_adapter

from . import _{{crate_name}}


class FormatAdapter:
    def read(self, path, **kwargs):
        return _{{crate_name}}.read_{{crate_name}}(str(path), **kwargs)


register_adapter("{{project-name}}", FormatAdapter(), extensions=[".{{crate_name}}"])
