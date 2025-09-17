import argparse
import re
from pathlib import Path

from include.pre_commit_hooks.sql_checks import SqlFix


class FixNoDatabaseQualifier(SqlFix):
    """
    In SQL procedures, we should not qualify with database
    """

    def __init__(self, *args, **kwargs):
        self._db = None
        super().__init__(*args, **kwargs)

    @property
    def db(self):
        if not self._db:
            self._db = re.sub(".*?/sql/", "", self.posix_path).split("/")[0]
        return self._db

    @staticmethod
    def unqualify_database(database, line):
        def remove(match):
            value = match.group()
            if value.startswith("'"):
                return value
            db, schema, table = value.split(".")
            if db.lower() == database.lower():
                return f"{schema}.{table}"
            else:
                return value

        return re.sub(r'([\'\w"]+\.[\w"]+\.[\'\w"]+)', remove, line)

    def fix_line(self, line, line_num) -> str:
        if self.db in ["edw"]:
            return self.unqualify_database(self.db, line)  # type: ignore
        return line  # type: ignore


def main(argv=None):
    parser = argparse.ArgumentParser(description="Ensure database is not qualified.")
    parser.add_argument("filenames", nargs="*", help="Filenames to check")
    args = parser.parse_args(argv)
    retv = 0

    for filename in args.filenames:
        status = FixNoDatabaseQualifier(Path(filename)).process_file()
        retv = retv or status

    return retv


if __name__ == "__main__":
    exit(main())
