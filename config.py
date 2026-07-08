import os

from dotenv import load_dotenv

load_dotenv()


def _require(name: str) -> str:
    value = os.environ.get(name)

    if not value:
        raise RuntimeError(f"Missing required env var {name}")

    return value


GCP_PROJECT = _require("GCP_PROJECT")
BRONZE_BUCKET = _require("BRONZE_BUCKET")
BQ_DATASET = _require("BQ_DATASET")
