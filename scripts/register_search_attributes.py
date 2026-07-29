"""Register the fleet's custom Search Attributes on the observed namespace.

Search Attributes must exist on the namespace before any workflow can set them:
a workflow that sets an unregistered attribute fails its workflow task. Run this
once per namespace, before bringing the fleet up.

    python -m scripts.register_search_attributes            # register
    python -m scripts.register_search_attributes --dry-run   # print the command

The single source of truth is common/search_attributes.ALL_KEYS: the name and
type of every attribute come from the same keys the workflows use, so the two
cannot drift.

Registration goes through `tcld`, the Temporal Cloud control-plane CLI (the
namespace-level operator API for adding Search Attributes is not available on
Cloud). It needs a Cloud API key with write access to the namespace; TEMPORAL_API_KEY
from .env is used unless TEMPORAL_CLOUD_API_KEY is set. Propagation takes a few
seconds and has no SLA, so register before you need it, not during a demo.
"""
from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys

from temporalio.api.enums.v1 import IndexedValueType

from common.config import load_config
from common.search_attributes import ALL_KEYS

# Python SDK indexed-value type -> the type name tcld expects.
_TCLD_TYPE = {
    IndexedValueType.INDEXED_VALUE_TYPE_TEXT: "Text",
    IndexedValueType.INDEXED_VALUE_TYPE_KEYWORD: "Keyword",
    IndexedValueType.INDEXED_VALUE_TYPE_INT: "Int",
    IndexedValueType.INDEXED_VALUE_TYPE_DOUBLE: "Double",
    IndexedValueType.INDEXED_VALUE_TYPE_BOOL: "Bool",
    IndexedValueType.INDEXED_VALUE_TYPE_DATETIME: "Datetime",
    IndexedValueType.INDEXED_VALUE_TYPE_KEYWORD_LIST: "KeywordList",
}


def attribute_specs() -> list[str]:
    """"Name=Type" for every attribute the fleet uses, sorted by name."""
    specs = []
    for key in ALL_KEYS:
        type_name = _TCLD_TYPE.get(key.indexed_value_type)
        if type_name is None:
            raise RuntimeError(f"unmapped search attribute type for {key.name}")
        specs.append(f"{key.name}={type_name}")
    return sorted(specs)


def build_command(namespace: str, api_key: str) -> list[str]:
    cmd = ["tcld", "--auto_confirm", "--idempotent"]
    if api_key:
        cmd += ["--api-key", api_key]
    cmd += ["namespace", "search-attributes", "add", "--namespace", namespace]
    for spec in attribute_specs():
        cmd += ["--sa", spec]
    return cmd


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="print what would be registered and exit",
    )
    args = parser.parse_args()

    config = load_config()
    namespace = os.environ.get("TEMPORAL_CLOUD_NAMESPACE") or config.namespace
    if not namespace:
        print("Missing TEMPORAL_NAMESPACE (see .env.example).", file=sys.stderr)
        return 2
    api_key = os.environ.get("TEMPORAL_CLOUD_API_KEY") or config.api_key

    specs = attribute_specs()
    print(f"{len(specs)} custom search attributes for {namespace}:")
    for spec in specs:
        name, type_name = spec.split("=")
        print(f"  {name:<22} {type_name}")

    cmd = build_command(namespace, api_key)
    if args.dry_run:
        # Never echo the key.
        printable = ["***" if part == api_key and api_key else part for part in cmd]
        print("\n" + " ".join(printable))
        return 0

    if shutil.which("tcld") is None:
        print(
            "\ntcld not found. Install it (brew install temporalio/brew/tcld) or add "
            "these attributes in the Cloud UI: Namespace -> Edit -> Custom Search "
            "Attributes.",
            file=sys.stderr,
        )
        return 1

    print("\nregistering via tcld ...", flush=True)
    result = subprocess.run(cmd)
    if result.returncode != 0:
        print(
            "\ntcld failed. Common causes: the API key lacks namespace write access, "
            "or an attribute already exists with a different type (Cloud cannot "
            "remove or retype an attribute).",
            file=sys.stderr,
        )
        return result.returncode

    print("done. Allow a few seconds for the attributes to become queryable.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
