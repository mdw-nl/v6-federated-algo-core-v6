from typing import Dict, Iterable, List, Optional

from .errors import ConfigError
from .types import MethodSpec


class MethodRegistry:
    def __init__(self, specs: Optional[Iterable[MethodSpec]] = None):
        self._specs: Dict[str, MethodSpec] = {}
        if specs:
            for spec in specs:
                self.register(spec)

    def register(self, spec: MethodSpec) -> MethodSpec:
        if spec.name in self._specs:
            raise ConfigError(f"Method '{spec.name}' is already registered")
        self._specs[spec.name] = spec
        return spec

    def get(self, method_name: str) -> MethodSpec:
        try:
            return self._specs[method_name]
        except KeyError as exc:
            raise ConfigError(f"Unknown method '{method_name}'") from exc

    def maybe_get(self, method_name: str) -> Optional[MethodSpec]:
        return self._specs.get(method_name)

    def names(self) -> List[str]:
        return sorted(self._specs)

    def __contains__(self, method_name: object) -> bool:
        return method_name in self._specs

    def __len__(self) -> int:
        return len(self._specs)
