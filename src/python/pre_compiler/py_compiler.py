from ast import Await, Yield, YieldFrom, Lambda, Module, Assign, Name, Expr, Store, Load, expr
import ast

__RUST_RESULT_TARGET_ID = '__rust_result'

def __rust_result_name(lineno:int):
    return Name(__RUST_RESULT_TARGET_ID, Store(), lineno=lineno, end_lineno=lineno, col_offset=0, end_col_offset=len(__RUST_RESULT_TARGET_ID))

def compile_code(tree: Module):
    return compile(tree, "<string>", mode="exec")

def try_assign_result_from_assignment(tree: Module, last_expression:Assign):
    last_expression: Assign = last_expression
    target: expr = last_expression.targets[0] if last_expression.targets else None

    if target and isinstance(target, Name):
        assign_syntax_op_len = 3 #' = '3 chars
        target:Name = target
        lineno = target.lineno + 1
        rust_name = __rust_result_name(lineno)
        col_offset = rust_name.end_col_offset + assign_syntax_op_len
        end_col_offset = col_offset + len(target.id)
        name_load_expr = Name(target.id, Load(), lineno=lineno, end_lineno=lineno, col_offset=col_offset, end_col_offset=end_col_offset)
        assign = Assign([rust_name], name_load_expr, lineno=lineno, end_lineno=lineno, col_offset=0, end_col_offset=end_col_offset)
        tree.body.append(assign)
        return tree

    return tree

def assign_generic_expression(tree: Module, last_expression:Expr):
    last_expression: Expr = last_expression
    assign = Assign([__rust_result_name(last_expression.lineno)], last_expression.value,
                    lineno=last_expression.lineno,
                    end_lineno=last_expression.end_lineno,
                    col_offset=last_expression.col_offset,
                    end_col_offset=last_expression.end_col_offset
                    )
    tree.body[-1] = assign
    return tree

def compile_str_code(code: str):
    tree: Module = ast.parse(code, mode="exec")

    last_expression = tree.body[-1] if len(tree.body) else None

    if not last_expression:
        return compile_code(tree)

    if not isinstance(last_expression, ast.Expr) and not isinstance(last_expression, Assign):
        return compile_code(tree)

    if type(last_expression) in (Await, Yield, YieldFrom, Lambda):
        return compile_code(tree)

    if isinstance(last_expression, Assign):
        return compile_code(try_assign_result_from_assignment(tree, last_expression))

    return compile_code(assign_generic_expression(tree, last_expression))



