"""Environment-driven configuration for the demo workload.

Reads connection and tuning knobs from the environment (a .env file is loaded
if present). Task queue names are not here; they are fixed constants in
constants.py.
"""
from __future__ import annotations

import os
from dataclasses import dataclass

try:
    from dotenv import load_dotenv

    load_dotenv()
except Exception:
    # dotenv is optional; environment may already be populated.
    pass


@dataclass(frozen=True)
class Config:
    address: str
    namespace: str
    api_key: str

    dashboard_host: str
    dashboard_port: int

    order_start_rate_per_sec: float
    order_start_jitter: float
    courier_fleet_size: int
    control_cache_refresh_sec: float


def load_config() -> Config:
    address = os.environ.get("TEMPORAL_ADDRESS", "")
    namespace = os.environ.get("TEMPORAL_NAMESPACE", "")
    api_key = os.environ.get("TEMPORAL_API_KEY", "")

    return Config(
        address=address,
        namespace=namespace,
        api_key=api_key,
        dashboard_host=os.environ.get("DASHBOARD_HOST", "127.0.0.1"),
        dashboard_port=int(os.environ.get("DASHBOARD_PORT", "8800")),
        order_start_rate_per_sec=float(os.environ.get("ORDER_START_RATE_PER_SEC", "0.6")),
        order_start_jitter=float(os.environ.get("ORDER_START_JITTER", "0.2")),
        courier_fleet_size=int(os.environ.get("COURIER_FLEET_SIZE", "10")),
        control_cache_refresh_sec=float(os.environ.get("CONTROL_CACHE_REFRESH_SEC", "2.5")),
    )
