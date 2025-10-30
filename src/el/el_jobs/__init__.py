from pathlib import Path
import sys

src_path = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(src_path))