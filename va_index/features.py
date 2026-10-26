"""Mock models; the forward data lookup and TCC reads are real."""

import hashlib

from .storage import SimulationError, object_value, read_json, stable_hash, text_value
from .regions import RegionalService, local_service, require_state_path


class FeatureService(RegionalService):
    va_only = True

    def __init__(self, config, forward_path):
        super().__init__(config.region)
        self.config = config
        self.forward_path = forward_path

    @local_service
    def compute(self, business_id: str, feature_name: str, mode="realtime") -> list[float]:
        text_value(business_id, "business_id")
        config = self.config.snapshot()
        feature = config.tcc["features"].get(feature_name)
        if feature is None:
            raise SimulationError(f"Unknown feature: {feature_name}")
        if mode == "realtime":
            source = {"business_id": business_id}
        elif mode == "forward":
            require_state_path(self.forward_path, self.region)
            forward = object_value(read_json(self.forward_path), "forward data")
            row = object_value(forward.get(business_id), f"forward data for {business_id}")
            if feature_name not in row:
                raise SimulationError(f"Missing forward feature {feature_name} for {business_id}")
            source = row[feature_name]
        else:
            raise SimulationError("mode must be realtime or forward")
        # Stable fake vectors: identical forward feature values produce equal vectors.
        seed = stable_hash([feature_name, feature["seed"], source])
        result = []
        for position in range(feature["dimension"]):
            digest = hashlib.sha256(f"{seed}:{position}".encode()).digest()
            result.append(round(int.from_bytes(digest[:4], "big") / (2**32 - 1) * 2 - 1, 6))
        return result
