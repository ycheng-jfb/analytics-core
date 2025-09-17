import argparse
from typing import Optional, Sequence

ALLOWED_LIST = {"Dockerfile", "docker/Dockerfile"}


def main(argv=None):  # type: (Optional[Sequence[str]]) -> int
    parser = argparse.ArgumentParser(description="Ensure filenames are lowercase.")
    parser.add_argument("filenames", nargs="*", help="Filenames to check")
    args = parser.parse_args(argv)

    retv = 0

    for filename in args.filenames:
        if filename in ALLOWED_LIST:
            continue
        if not filename == filename.lower():
            print(f"{filename}: filename must be lowercase")
            retv = 1

    return retv


if __name__ == "__main__":
    exit(main())
