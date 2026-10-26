"""Logical service-region boundaries for the local simulator (not an OS sandbox)."""

from contextlib import contextmanager
from contextvars import ContextVar
from functools import wraps
from pathlib import Path
import re

from .storage import SimulationError, file_lock, read_json, write_json


_caller_region = ContextVar("caller_region", default=None)


def region_name(value):
    if not isinstance(value, str) or not re.fullmatch(r"[a-z][a-z0-9_-]{0,31}", value):
        raise SimulationError(f"Invalid region: {value!r}")
    return value


def require_local(region, resource):
    caller = _caller_region.get()
    if caller is not None and caller != region:
        raise SimulationError(f"Cross-region access denied: {caller} -> {region} {resource}")


def require_va(region, resource):
    require_local(region, resource)
    if region != "va":
        raise SimulationError(f"{resource} is deployed only in va; unavailable in {region}")


def require_control_plane():
    if _caller_region.get() is not None:
        raise SimulationError("A regional service cannot invoke the cluster control plane")


class RegionalService:
    """A deployment identity, independent of replaceable clients/configuration."""

    va_only = False

    def __init__(self, region):
        self._region = region_name(region)

    @property
    def region(self):
        return self._region

    @property
    def available(self):
        return not self.va_only or self.region == "va"


def local_service(function):
    @wraps(function)
    def call(self, *args, **kwargs):
        require_local(self.region, function.__qualname__)
        with region_scope(self.region):
            if self.va_only:
                require_va(self.region, type(self).__name__)
            return function(self, *args, **kwargs)
    return call


@contextmanager
def region_scope(region):
    region_name(region)
    require_local(region, "service execution")
    token = _caller_region.set(region)
    try:
        yield
    finally:
        _caller_region.reset(token)


@contextmanager
def _mirror_handoff(region):
    """Only the Mirror transport uses this to enter a destination broker."""
    token = _caller_region.set(region)
    try:
        yield
    finally:
        _caller_region.reset(token)


def claim_state(directory, region):
    require_local(region, "state directory")
    with file_lock(directory / ".region.lock"):
        marker = directory / ".region.json"
        if marker.exists():
            if read_json(marker).get("region") != region:
                raise SimulationError(f"State directory {directory} belongs to another region")
        else:
            # Older VA state has no marker; don't relabel it as another region.
            if region != "va" and any(path.name != ".region.lock" for path in directory.iterdir()):
                raise SimulationError("Nonempty unmarked state must be opened as the legacy VA deployment")
            write_json(marker, {"region": region})


def require_state_path(path, region):
    """Check the owner of a managed state path, including symlink destinations.

    Unmarked paths remain usable by standalone components/fixtures. Runtime
    always claims its state before constructing components.
    """
    require_local(region, "state storage")
    resolved = Path(path).resolve()
    for directory in (resolved, *resolved.parents):
        marker = directory / ".region.json"
        if marker.is_file():
            owner = region_name(read_json(marker).get("region"))
            if owner != region:
                raise SimulationError(f"Cross-region storage access denied: {region} -> {owner}: {path}")
            return
