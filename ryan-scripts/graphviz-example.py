from graphviz import Digraph
import ast


def extract_function_flow(filepath: str) -> Digraph:
    """Parse a Python file and generate a function flow diagram."""
    with open(filepath, "r") as file:
        tree = ast.parse(file.read())

    # Initialize graph
    flowchart = Digraph(format="png")
    flowchart.attr(rankdir="LR")  # Horizontal layout

    functions = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef):
            func_name = node.name
            functions[func_name] = [
                n.id for n in ast.walk(node) if isinstance(n, ast.Name)
            ]

    # Create nodes and edges
    for func, calls in functions.items():
        flowchart.node(func, func)
        for call in calls:
            if call in functions:  # Only include defined functions
                flowchart.edge(func, call)

    return flowchart


# Example usage
script_path = r"Q:\Library\Automation\ryan-tools\ryan-scripts\TUFLOW-python\POMM_combine_v11.py"  # Replace with your Python script path
diagram = extract_function_flow(script_path)
diagram.render("function_flow_diagram", cleanup=True)
