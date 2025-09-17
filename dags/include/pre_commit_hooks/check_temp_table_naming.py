import argparse
import re
from pathlib import Path

from include.pre_commit_hooks.sql_checks import SqlCheck


class CheckTempTable(SqlCheck):
    """
    Checks that temp tables are not qualified with database or schema and have a leading underscore.
    """

    @staticmethod
    def evaluate_temp_table_name(table_name):
        issues = []
        m = re.match(r"^_[a-zA-Z0-9_]+$", table_name)
        if not m:
            issues.append("must begin with _ and no database or schema")
        return issues

    @staticmethod
    def get_table_name(line):
        m = re.match(
            r"create\s+(?:or\s+replace\s+)"
            r"?(?:temp|temporary)\s+table\s+"
            r"(?:if\s+not\s+exists\s+)"
            r'?([\w".]+)\s*(?:as|\(|$)',
            line,
            flags=re.IGNORECASE | re.MULTILINE,
        )
        if m:
            table = m.group(1)
            return table

    def check_line(self, line, line_num) -> int:
        table = self.get_table_name(line)
        if table:
            issues = self.evaluate_temp_table_name(table)
            if issues:
                print(line)
                print(f"bad temp table name `{table}`: {'; '.join(issues)}")
                return 1
        return 0


def main(argv=None):
    parser = argparse.ArgumentParser(description="Check temp table naming.")
    parser.add_argument("filenames", nargs="*", help="Filenames to check")
    args = parser.parse_args(argv)
    retv = 0

    for filename in args.filenames:
        status = CheckTempTable(Path(filename)).process_file()
        retv = retv or status

    return retv


if __name__ == "__main__":
    exit(main())
