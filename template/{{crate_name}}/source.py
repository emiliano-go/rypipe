from rypipe import Adapter

from . import _{{crate_name}}


class AdapterSource(Adapter):
    def read(self, path, **kwargs):
        return _{{crate_name}}.read_{{crate_name}}(path, **kwargs)
