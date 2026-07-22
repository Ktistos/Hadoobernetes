import sys
from pathlib import Path

# Add subfolder directories to sys.path so tests can be run from the root
root = Path(__file__).resolve().parent
sys.path.insert(0, str(root / "cluster_manager"))
sys.path.insert(0, str(root / "job_master"))
sys.path.insert(0, str(root / "worker"))
