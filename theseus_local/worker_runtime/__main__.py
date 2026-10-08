"""Run the standalone persistent worker with ``python -m theseus_local.worker_runtime``."""

from .entrypoint import main


if __name__ == "__main__":
    raise SystemExit(main())
