import argparse
import re
from pathlib import Path

from include.pre_commit_hooks.sql_checks import SqlFix


class FixQuotedIdentifiers(SqlFix):
    """
    Ensures that object identifiers are only quoted when strictly necessary.
    """

    exclusion_list = {f'"{x}"' for x in ["ORDER", "GROUP"]}

    @classmethod
    def unquote_identifier(cls, match):
        value = match.group()
        if value in cls.exclusion_list:
            return value
        return value.lower().replace('"', "")

    def fix_line(self, line, line_num) -> str:
        return re.sub(r'("[A-Z_]+[A-Z0-9_]*")', self.unquote_identifier, line)  # type: ignore


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Unquote identifiers unless necessary."
    )
    parser.add_argument("filenames", nargs="*", help="Filenames to check")
    args = parser.parse_args(argv)
    retv = 0

    for filename in args.filenames:
        status = FixQuotedIdentifiers(Path(filename)).process_file()
        retv = retv or status

    return retv


if __name__ == "__main__":
    exit(main())
