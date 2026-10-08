"""Command-line entry point for the loopback campaign UI."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from .app import CampaignUiApplication
from .client import PublicApiCampaignClient
from .local_control import DEFAULT_UI_STATE_DIR, LocalWorkspaceCampaignClient
from .server import CampaignUiHttpServer


def build_parser() -> argparse.ArgumentParser:
    # Build the dependency-free UI command parser.
    parser = argparse.ArgumentParser(prog="theseus-ui")
    parser.add_argument("campaign_database", nargs="?", help="legacy single-campaign SQLite database")
    parser.add_argument("--state-dir", default=str(DEFAULT_UI_STATE_DIR), help="browser project registry and campaign state root")
    parser.add_argument("--project-root", action="append", default=[], help="register a project at startup; may be repeated")
    parser.add_argument("--knowledge-database")
    parser.add_argument("--statistics-database")
    parser.add_argument("--port", type=int, default=0)
    return parser


def main(argv: list[str] | None = None) -> int:
    # Serve the campaign UI until interrupted without binding a non-loopback interface.
    args = build_parser().parse_args(argv)
    if args.campaign_database:
        client = PublicApiCampaignClient(
            Path(args.campaign_database),
            knowledge_database=Path(args.knowledge_database) if args.knowledge_database else None,
            statistics_database=Path(args.statistics_database) if args.statistics_database else None,
        )
    else:
        client = LocalWorkspaceCampaignClient(
            args.state_dir,
            project_roots=tuple(args.project_root),
        )
    server = CampaignUiHttpServer(args.port, CampaignUiApplication(client))
    port = int(server.server_address[1])
    print(f"http://127.0.0.1:{port}/", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        return 0
    finally:
        server.server_close()


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
