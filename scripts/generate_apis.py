"""Regenerate Cordium and shared metadata from a local protobuf checkout.

Usage: python scripts/generate_apis.py /path/to/pb
Requires protoc and betterproto[compiler]==2.0.0b7 in the active environment.
Only the two generated modules are replaced; package metadata is preserved.
"""

import argparse
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("protobuf_root", type=Path)
args = parser.parse_args()
root = Path(__file__).resolve().parents[1]
plugin = Path(sys.executable).parent / "protoc-gen-python_betterproto"
with tempfile.TemporaryDirectory() as directory:
    subprocess.run(
        [
            "protoc",
            f"-I{args.protobuf_root.resolve()}",
            f"--plugin=protoc-gen-python_betterproto={plugin}",
            f"--python_betterproto_out={directory}",
            "apis/protobuf/main/metav1/metav1.proto",
            "apis/protobuf/main/cordiumv1/cordiumv1.proto",
        ],
        check=True,
    )
    for name in ("meta", "cordium"):
        relative = Path("octelium/api/main") / name / "v1/__init__.py"
        shutil.copyfile(Path(directory) / relative, root / "packages/apis" / relative)
