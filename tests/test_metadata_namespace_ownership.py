"""Site-specific guards against the replaced ambient namespace mechanism."""

import ast
import inspect

from polystore.virtual_workspace import VirtualWorkspaceBackend


def test_workspace_retains_its_namespace_owner_not_a_filename_projection():
    tree = ast.parse(inspect.getsource(VirtualWorkspaceBackend))
    calls = [node for node in ast.walk(tree) if isinstance(node, ast.Call)]
    assert not any(
        isinstance(call.func, ast.Name) and call.func.id == "get_metadata_path" for call in calls
    )
    namespace_path_calls = [
        call
        for call in calls
        if isinstance(call.func, ast.Attribute) and call.func.attr == "metadata_path"
    ]
    assert len(namespace_path_calls) == 1
    assert ast.unparse(namespace_path_calls[0].func.value) == "self.metadata_config"
    methods = {node.name: node for node in tree.body[0].body if isinstance(node, ast.FunctionDef)}
    assert "self.metadata_config" in ast.unparse(methods["get_connection_params"])
    for method in ("from_connection_params", "set_connection_params"):
        assert "params['metadata_config']" in ast.unparse(methods[method])
