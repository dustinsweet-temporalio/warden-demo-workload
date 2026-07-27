"""Temporal Cloud connection helper (API key form, verified in the brief).

The namespace is the full <namespace>.<account> id. The address is
<namespace>.<account>.tmprl.cloud:7233.
"""
from __future__ import annotations

from temporalio.client import Client

from common.config import Config


async def connect(config: Config) -> Client:
    if not config.address or not config.namespace or not config.api_key:
        raise RuntimeError(
            "Missing connection config. Set TEMPORAL_ADDRESS, TEMPORAL_NAMESPACE, "
            "and TEMPORAL_API_KEY (see .env.example)."
        )
    return await Client.connect(
        config.address,
        namespace=config.namespace,
        api_key=config.api_key,
        tls=True,
        rpc_metadata={"temporal-namespace": config.namespace},
    )
