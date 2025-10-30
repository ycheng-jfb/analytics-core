import argparse
from pathlib import Path
from typing import List, Tuple, Union

# CST - Concrete Syntax Tree - represents source code as a parsed token tree,
# including additional info on whitespaces, comments, etc.
# this is required to maintain existing code formatting when modifying source files
import libcst as cst


class DAGVariableDefinitionVisitor(cst.CSTVisitor):
    """
    Class to get variable name pointing to DAG() instance
    """

    METADATA_DEPENDENCIES = (cst.metadata.PositionProvider,)

    def __init__(self):
        self.dag_var = ''
        self.lineno = 0
        super().__init__()

    def visit_Assign(self, node: cst.Assign):
        """
        visit assignments of the below form and get var_name:
            <var_name> = DAG(
                dag_id='DAG_ID',
                ...

        )

        Args:
            node (cst.Assign): CST Assign node [depicts an assignment statement]

        """
        if (
            isinstance(node.value, cst.Call)
            and isinstance(node.value.func, cst.Name)
            and node.value.func.value == 'DAG'
        ):
            self.dag_var = node.targets[0].target.value  # type: ignore
            self.lineno = self.get_metadata(cst.metadata.PositionProvider, node).start.line


class DAGContextManagerVisitor(cst.CSTVisitor):
    """
    Class to get line number at which the 'with dag:' context block starts
    """

    METADATA_DEPENDENCIES = (cst.metadata.PositionProvider,)

    def __init__(self, dag_var: str):
        self.dag_var = dag_var
        self.lineno = 0
        super().__init__()

    def visit_With(self, node: cst.With):
        """
        visit all with clauses and get lineno where dag variable is used
        Eg:
        `with <dag_variable>: capture this lineno`
        `    op1 = SomeOperator(*args, **kwargs)`
        `    ...`

        Args:
            node (cst.With): CST With node [depicts with clause as shown above]

        """
        # if dag variable was found, search for `with <dag_var>:` line
        if self.dag_var:
            for item in node.items:
                if isinstance(item.item, cst.Name) and item.item.value == self.dag_var:
                    self.lineno = self.get_metadata(cst.metadata.PositionProvider, node).start.line
                    break
        # else, search for `with DAG()` line
        else:
            for item in node.items:
                if isinstance(item.item, cst.Call) and item.item.func.value == 'DAG':
                    self.lineno = self.get_metadata(cst.metadata.PositionProvider, node).start.line
                    break


class OperatorExecuteCallRemover(cst.CSTTransformer):
    """
    Capture execute() calls after 'with dag:' context block starts
    """

    METADATA_DEPENDENCIES = (cst.metadata.PositionProvider,)

    def __init__(self, lineno: int):
        self.lineno = lineno
        self.warn_list: List[Tuple[str, int]] = []
        super().__init__()

    def leave_Expr(
        self, node: cst.Expr, updated_node: cst.Expr
    ) -> Union[cst.Expr, cst.RemovalSentinel]:
        """
        visit all expressions after the 'with <dag_variable>:' clause
        and look for .execute() calls

        Args:
            node (cst.Expr): current expression node
            updated_node (cst.Expr):
                new node which will replace current node in CST when leave_Expr() exits
        """
        lineno = self.get_metadata(cst.metadata.PositionProvider, node).start.line
        if lineno > self.lineno:
            if (
                isinstance(node.value, cst.Call)  # type: ignore
                and isinstance(node.value.func, cst.Attribute)  # type: ignore
                and node.value.func.attr.value == 'execute'  # type: ignore
            ):
                self.warn_list.append((cst.Module([]).code_for_node(node), lineno))
                # cst.RemoveFromParent() -> tells the CST parser that this node will be removed
                return cst.RemoveFromParent()

        return updated_node


class CheckOperatorExecuteCalls:
    def __init__(self, files: List[str]):
        self.files = map(Path, files)
        super().__init__()

    @staticmethod
    def _validate_file(file: Path) -> List[Tuple[str, str, int]]:
        """Static method to check and remove execute() calls for one file

        Args:
            file (Path): file to verify

        Returns:
            List[Tuple[str, str, int]]:
                a list of (file_name, code_with_execute_call, line_number_of_execute_call) tuples
        """
        warn_list: List[Tuple[str, str, int]] = []
        code = file.read_text()
        tree = cst.metadata.MetadataWrapper(cst.parse_module(code))

        # get name of variable containing reference to DAG() instance
        dag_def_visitor = DAGVariableDefinitionVisitor()
        _ = tree.visit(dag_def_visitor)

        # get lineno of 'with <dag_var>:' clause
        dag_ctx_mgr_visitor = DAGContextManagerVisitor(dag_var=dag_def_visitor.dag_var)
        _ = tree.visit(dag_ctx_mgr_visitor)

        # if both dag variable and dag context manager are not found, return
        if dag_ctx_mgr_visitor.lineno == 0 and dag_def_visitor.lineno == 0:
            return warn_list

        # remove .execute() calls after dag_ctx_mgr_visitor.lineno or dag_def_visitor.lineno
        execute_call_remover = OperatorExecuteCallRemover(
            lineno=(dag_ctx_mgr_visitor.lineno or dag_def_visitor.lineno)
        )
        new_tree = tree.visit(execute_call_remover)

        if execute_call_remover.warn_list:
            warn_list = [(f'{file}', i[0], i[1]) for i in execute_call_remover.warn_list]
            file.write_text(new_tree.code)

        return warn_list

    def validate_files(self) -> int:
        """Helper method to run _validate_files() for all staged files

        Returns:
            int: status_code - 1 if there are any execute() calls removed by this hook
        """
        warn_list = []
        for file in self.files:
            warn_list.extend(self._validate_file(file))

        if warn_list:
            print('.execute() calls found! Removing...')
            for file_name, code, lineno in warn_list:
                print(f'{file_name} - at line {lineno} - {code}')
            return 1
        return 0


def main(argv=None):
    parser = argparse.ArgumentParser(
        description='Flag and remove any manual operator.execute() calls in DAG files'
    )
    parser.add_argument('filenames', nargs='*', help='Filenames to check')
    args = parser.parse_args(argv)

    return CheckOperatorExecuteCalls(files=args.filenames).validate_files()


if __name__ == '__main__':
    exit(main())
