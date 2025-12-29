"""
Main entrypoint. Simple CLI to run daily or test.
"""

import sys
from core.logger import get_logger
from scripts.run_daily import __name__ as _rd
from pipeline.orchestrator import run_pipeline

logger = get_logger("main")

def print_help():
    print("Usage:")
    print("  python main.py run    # run full pipeline once")
    print("  python main.py test <url>   # run single-URL test")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print_help()
        sys.exit(1)
    cmd = sys.argv[1]
    if cmd == "run":
        run_pipeline()
    elif cmd == "test":
        if len(sys.argv) < 3:
            print("Provide a URL to test")
            sys.exit(1)
        from scripts.test_source import main as test_main
        test_main(sys.argv[2], sys.argv[3] if len(sys.argv) > 3 else "test")
    else:
        print_help()
