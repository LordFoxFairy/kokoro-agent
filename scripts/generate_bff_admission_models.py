"""Generate the fixed, narrow BFF budget consumer; no other BFF RPC is exposed."""

import argparse
import json
import hashlib
from pathlib import Path
from kokoro_agent.bff_admission_contract import (
    generate_bff_admission_models,
    bff_admission_receipt,
    validate_bff_admission_consumer,
    SOURCE,
    GENERATED,
    OWNER_SHA256,
)
from kokoro_agent.execution_proof_contract import OWNER_SOURCE_FILES

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    generate_bff_admission_models(root, check=args.check)
    if args.check:
        validate_bff_admission_consumer(root)
    else:
        path = root / "contract/provenance.json"
        doc = json.loads(path.read_text())
        doc["consumer_dependencies"] = [
            r for r in doc["consumer_dependencies"] if r["owner"] != "kokoro-bff"
        ] + [bff_admission_receipt()]
        doc["generated_artifacts"] = [
            r for r in doc["generated_artifacts"] if r["path"] != GENERATED
        ] + [
            {
                "path": GENERATED,
                "sha256": hashlib.sha256((root / GENERATED).read_bytes()).hexdigest(),
                "source": SOURCE,
                "source_sha256": OWNER_SHA256,
                "generator": "scripts/generate_bff_admission_models.py",
            }
        ]
        digest = hashlib.sha256()
        for source in OWNER_SOURCE_FILES:
            digest.update((root / source).read_bytes())
        doc["source_files"] = list(OWNER_SOURCE_FILES)
        doc["combined_sha256"] = digest.hexdigest()
        path.write_text(json.dumps(doc, indent=2) + "\n")
