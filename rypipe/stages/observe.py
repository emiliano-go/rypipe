"""Observer-backed stage base: side effects that survive fusion.

A stage whose only job is a per-row side effect (logging, metrics, audit)
normally loses that side effect when the pipeline fuses: fused stages never
call ``apply()``. ``ObservedStage`` instead returns observer hooks from
``_plan_kwargs()``, so the hooks are pushed into the parse loop and keep
firing when fused.
"""

from __future__ import annotations


class ObservedStage:
    """Base class for stages implemented as observer hooks.

    Subclasses override :meth:`observer_hooks` to return a dict like
    ``{"on_row_rejected": fn}``. Valid keys: ``on_begin_row``,
    ``on_put_field``, ``on_row_accepted``, ``on_row_rejected``,
    ``on_chunk_finished``.

    Hooks fire from parse threads (possibly several at once for parallel
    engines), so they must be thread-safe and cheap. Exceptions raised by a
    hook are printed and swallowed: a hook can never abort a parse.
    ``on_put_field`` takes the GIL per field; prefer row-level hooks from
    Python and keep field-level hooks to Rust.

    When a pipeline cannot fuse the stage, override ``apply()`` as the
    Python fallback for the same effect.
    """

    def observer_hooks(self) -> dict:
        """Return the observer hook dict, e.g. ``{"on_row_rejected": fn}``."""
        return {}

    def _plan_kwargs(self) -> dict | None:
        hooks = self.observer_hooks()
        return {"observer": hooks} if hooks else None
