from pathlib import Path


class SqlFix:
    """
    Fixes are used when can actually fix the problems we find.

    Should return the corrected line.
    """

    def __init__(self, file: Path):
        self.file = file
        self.posix_path = file.resolve().as_posix()
        self.retval = 0

    def process_file(self) -> int:
        content = self.file.read_text()
        new_content_list = []
        for idx, line in enumerate(content.splitlines(keepends=True)):
            new_line = self.fix_line(line, idx + 1)
            new_content_list.append(new_line)
        new_content = ''.join(new_content_list)
        if new_content != content:
            self.file.write_text(new_content)
            print(f"updated: {self.posix_path}")
            self.retval = 1
        return self.retval

    def fix_line(self, line: str, line_num: int) -> str:
        pass


class SqlCheck:
    """
    Checks are when we want to fail the pre-commit check but can't practically apply a fix.

    Should return 1 if bad or 0 if good.
    """

    def __init__(self, file: Path):
        self.file = file
        self.posix_path = file.resolve().as_posix()
        self.retval = 0

    def process_file(self) -> int:
        content = self.file.read_text()
        for idx, line in enumerate(content.splitlines(keepends=True)):
            if self.check_line(line, idx + 1):
                print(f"file: {self.posix_path}:{idx + 1}")
                self.retval = 1
        return self.retval

    def check_line(self, line: str, line_num: int) -> int:
        pass
