"""
Schema Manager CLI — manage Qdrant collections.

Usage:
    python schema_manager.py create   [--recreate]
    python schema_manager.py info
    python schema_manager.py delete
    python schema_manager.py list
"""
from __future__ import annotations

import argparse
import json
import logging
import sys

import config
import qdrant_utils

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def cmd_create(args: argparse.Namespace) -> None:
    qdrant_utils.ensure_collection(recreate=args.recreate)
    info = qdrant_utils.collection_info()
    print(json.dumps(info, indent=2))


def cmd_info(_args: argparse.Namespace) -> None:
    try:
        info = qdrant_utils.collection_info()
        print(json.dumps(info, indent=2))
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)


def cmd_delete(_args: argparse.Namespace) -> None:
    qdrant_utils.delete_collection()
    print(f"Collection '{config.QDRANT_COLLECTION}' deleted.")


def cmd_list(_args: argparse.Namespace) -> None:
    client = qdrant_utils.get_client()
    collections = client.get_collections().collections
    if not collections:
        print("No collections found.")
        return
    for col in collections:
        print(f"  • {col.name}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Qdrant Schema Manager")
    sub = parser.add_subparsers(dest="command", required=True)

    p_create = sub.add_parser("create", help="Create collection")
    p_create.add_argument("--recreate", action="store_true", help="Drop and re-create")
    p_create.set_defaults(func=cmd_create)

    p_info = sub.add_parser("info", help="Show collection info")
    p_info.set_defaults(func=cmd_info)

    p_delete = sub.add_parser("delete", help="Delete collection")
    p_delete.set_defaults(func=cmd_delete)

    p_list = sub.add_parser("list", help="List all collections")
    p_list.set_defaults(func=cmd_list)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
