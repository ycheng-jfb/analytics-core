import argparse
import re
from pathlib import Path

from include.pre_commit_hooks.sql_checks import SqlFix


class FixInnerJoin(SqlFix):
    """
    Replaces INNER JOIN with JOIN
    """

    def fix_line(self, line, line_num) -> str:
        return re.sub(r'inner\s+join', 'JOIN', line, flags=re.IGNORECASE)  # type: ignore


def main(argv=None):
    parser = argparse.ArgumentParser(description='Replace inner join with join.')
    parser.add_argument('filenames', nargs='*', help='Filenames to check')
    args = parser.parse_args(argv)
    retv = 0

    for filename in args.filenames:
        status = FixInnerJoin(Path(filename)).process_file()
        retv = retv or status

    return retv


if __name__ == '__main__':
    exit(main())
