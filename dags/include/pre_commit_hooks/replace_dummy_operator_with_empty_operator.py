import argparse
from pathlib import Path
from typing import List, Union

import libcst as cst


class DummyOperatorReplacer(cst.CSTTransformer):
    """
    Does the following replacements:
        DummyOperator -> EmptyOperator
        imports: airflow.operators.dummy -> airflow.operators.empty
    """

    def __init__(self):
        self.modified: bool = False
        super().__init__()

    def leave_Name(self, original_node: cst.Name, updated_node: cst.Name) -> cst.BaseExpression:
        if original_node.value == 'DummyOperator':
            self.modified = True
            return updated_node.with_changes(value='EmptyOperator')
        return updated_node

    def leave_ImportFrom(
        self, original_node: cst.ImportFrom, updated_node: cst.ImportFrom
    ) -> Union[
        cst.BaseSmallStatement, cst.FlattenSentinel[cst.BaseSmallStatement], cst.RemovalSentinel
    ]:
        if isinstance(original_node.module, cst.Attribute):
            if isinstance(original_node.module.value, cst.Attribute):
                if original_node.module.value.value.value == 'airflow':  # type: ignore
                    if original_node.module.value.attr.value == 'operators':
                        if original_node.module.attr.value == 'dummy':
                            self.modified = True
                            updated_node = updated_node.deep_replace(
                                updated_node.module.attr,  # type: ignore
                                cst.Name(value='empty', lpar=[], rpar=[]),  # type: ignore
                            )
        return updated_node


class ReplaceDummyOpWithEmptyOp:
    def __init__(self, files: List[str]):
        self.files = map(Path, files)
        super().__init__()

    @staticmethod
    def _validate_file(file: Path) -> bool:
        """Static method to check and remove DummyOperator usage

        Args:
            file (Path): file to verify

        Returns:
            boolean value indicating if any replacements were done
        """
        code = file.read_text()
        tree = cst.parse_module(code)

        dummy_op_replacer = DummyOperatorReplacer()
        new_tree = tree.visit(dummy_op_replacer)

        if dummy_op_replacer.modified:
            file.write_text(new_tree.code)

        return dummy_op_replacer.modified

    def validate_files(self) -> int:
        """Helper method to run _validate_files() for all staged files

        Returns:
            int: status_code - 1 if any DummyOperator usages are replaced by this hook
        """
        modified = any(list(map(self._validate_file, self.files)))

        if modified:
            print('DummyOperator usage found! Replacing with EmptyOperator...')
            return 1
        return 0


def main(argv=None):
    parser = argparse.ArgumentParser(description='Replace DummyOperator with EmptyOperator in code')
    parser.add_argument('filenames', nargs='*', help='Filenames to check')
    args = parser.parse_args(argv)

    return ReplaceDummyOpWithEmptyOp(files=args.filenames).validate_files()


if __name__ == '__main__':
    exit(main())
