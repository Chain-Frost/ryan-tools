import ast
import argparse
import logging
from graphviz import Digraph

# ===========================
# Default Configuration
# ===========================

# Default file path to analyze
DEFAULT_FILEPATH = r"E:\Library\Automation\ryan-tools\ryan-scripts\misc-python\parallel-reorder-xyz-updated.py"

# Default output filename (without extension)
DEFAULT_OUTPUT = "function_flow_diagram"

# ===========================
# Logging Configuration
# ===========================

# Configure logging for better error messages and debugging
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


def extract_function_calls(node: ast.AST) -> list[str]:
    """
    Recursively extract function calls within an AST node.

    Args:
        node (ast.AST): The AST node to traverse.

    Returns:
        list[str]: A list of function names that are called within the node.
    """
    calls = []
    for child in ast.iter_child_nodes(node):
        # Check if the child node is a function call
        if isinstance(child, ast.Call):
            # Handle different types of function calls
            if isinstance(child.func, ast.Name):
                # Simple function call: func()
                calls.append(child.func.id)
            elif isinstance(child.func, ast.Attribute):
                # Method call: obj.method()
                calls.append(child.func.attr)
        # Recursively extract calls from child nodes
        calls.extend(extract_function_calls(child))
    return calls


def extract_assigned_variables(node: ast.AST) -> set[str]:
    """
    Recursively extract variable names that are assigned within an AST node.

    Args:
        node (ast.AST): The AST node to traverse.

    Returns:
        set[str]: A set of variable names that are assigned within the node.
    """
    assigned_vars = set()
    for child in ast.walk(node):
        if isinstance(child, ast.Assign):
            for target in child.targets:
                if isinstance(target, ast.Name):
                    assigned_vars.add(target.id)
                elif isinstance(target, ast.Tuple):
                    for elt in target.elts:
                        if isinstance(elt, ast.Name):
                            assigned_vars.add(elt.id)
        elif isinstance(child, ast.AugAssign):
            if isinstance(child.target, ast.Name):
                assigned_vars.add(child.target.id)
        elif isinstance(child, ast.AnnAssign):
            if isinstance(child.target, ast.Name):
                assigned_vars.add(child.target.id)
    return assigned_vars


def extract_used_variables(node: ast.AST) -> set[str]:
    """
    Recursively extract variable names that are used (read) within an AST node.

    Args:
        node (ast.AST): The AST node to traverse.

    Returns:
        set[str]: A set of variable names that are used within the node.
    """
    used_vars = set()
    for child in ast.walk(node):
        if isinstance(child, ast.Name):
            if isinstance(child.ctx, ast.Load):
                used_vars.add(child.id)
    return used_vars


def extract_function_flow(filepath: str) -> Digraph:
    """
    Parse a Python file and generate a function and variable flow diagram.

    Args:
        filepath (str): The path to the Python file to analyze.

    Returns:
        Digraph: A Graphviz Digraph object representing the function and variable flow.
    """
    try:
        with open(filepath, "r", encoding="utf-8") as file:
            source = file.read()
            logging.info(f"Successfully read the file: {filepath}")
    except FileNotFoundError:
        logging.error(f"File not found: {filepath}")
        exit(1)
    except IOError as e:
        logging.error(f"IO error when reading the file: {e}")
        exit(1)

    try:
        # Parse the source code into an AST
        tree = ast.parse(source, filename=filepath)
        logging.info("Successfully parsed the source code into AST.")
    except SyntaxError as e:
        logging.error(f"Syntax error in the file: {e}")
        exit(1)

    # Initialize a directed graph
    flowchart = Digraph(name="Function and Variable Flow", format="png")
    flowchart.attr(rankdir="LR")  # Left to Right layout
    flowchart.attr(
        "node", shape="box", style="filled", color="lightblue2", fontname="Helvetica"
    )

    functions: dict[str, list[str]] = {}
    function_assigned_vars: dict[str, set[str]] = {}
    function_used_vars: dict[str, set[str]] = {}
    global_assigned_vars: set[str] = set()
    global_used_vars: set[str] = set()

    # First, identify global assignments
    for node in tree.body:
        if (
            isinstance(node, ast.Assign)
            or isinstance(node, ast.AugAssign)
            or isinstance(node, ast.AnnAssign)
        ):
            vars_assigned = extract_assigned_variables(node)
            global_assigned_vars.update(vars_assigned)
            used_vars = extract_used_variables(node)
            global_used_vars.update(used_vars)
            logging.debug(f"Global variables assigned: {vars_assigned}")
        elif isinstance(node, ast.FunctionDef):
            # Will handle functions later
            continue

    # Traverse the AST to find all function definitions
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef):
            func_name = node.name
            logging.debug(f"Found function definition: {func_name}")
            # Extract all function calls within this function
            calls = extract_function_calls(node)
            functions[func_name] = calls

            # Extract variables assigned within the function
            assigned_vars = extract_assigned_variables(node)
            function_assigned_vars[func_name] = assigned_vars
            logging.debug(f"Variables assigned in {func_name}: {assigned_vars}")

            # Extract variables used within the function
            used_vars = extract_used_variables(node)
            function_used_vars[func_name] = used_vars
            logging.debug(f"Variables used in {func_name}: {used_vars}")

    if not functions and not global_assigned_vars:
        logging.warning("No functions or variables found in the provided file.")
        # Create a graph with only Start and End
        flowchart.node(
            "Start",
            shape="ellipse",
            style="filled",
            color="green",
            fontname="Helvetica-Bold",
        )
        flowchart.node(
            "End",
            shape="ellipse",
            style="filled",
            color="red",
            fontname="Helvetica-Bold",
        )
        flowchart.edge("Start", "End", label="no functions or variables", fontsize="10")
        return flowchart

    # Create Start and End nodes
    flowchart.node(
        "Start",
        shape="ellipse",
        style="filled",
        color="green",
        fontname="Helvetica-Bold",
    )
    flowchart.node(
        "End", shape="ellipse", style="filled", color="red", fontname="Helvetica-Bold"
    )

    # Create nodes for each function
    for func in functions:
        flowchart.node(
            func, shape="box", style="filled", color="lightblue2", fontname="Helvetica"
        )

    # Create nodes for each global variable
    for var in global_assigned_vars:
        flowchart.node(
            var,
            shape="oval",
            style="filled",
            color="yellow",
            fontname="Helvetica",
            label=f"{var}\n(GLOBAL)",
        )

    # Create nodes for variables assigned within functions
    for func, vars_assigned in function_assigned_vars.items():
        for var in vars_assigned:
            flowchart.node(
                var,
                shape="oval",
                style="filled",
                color="lightgreen",
                fontname="Helvetica",
                label=f"{var}\n({func})",
            )

    # Create edges based on function calls
    for func, calls in functions.items():
        for call in calls:
            if call in functions:
                flowchart.edge(func, call, label="calls", fontsize="10")
                logging.debug(f"Added edge from {func} to {call}")
            else:
                # Handle external or undefined function calls
                flowchart.node(
                    call,
                    shape="box",
                    style="filled",
                    color="orange",
                    fontname="Helvetica",
                    label=call,
                )
                flowchart.edge(func, call, label="calls", fontsize="10")
                logging.debug(f"Added external edge from {func} to {call}")

    # Connect Start to the first function (if exists)
    if functions:
        first_func = list(functions.keys())[0]
        flowchart.edge("Start", first_func, label="starts", fontsize="10")

        # Connect the last function to End
        last_func = list(functions.keys())[-1]
        flowchart.edge(last_func, "End", label="ends", fontsize="10")

    # Connect functions to variables they assign
    for func, vars_assigned in function_assigned_vars.items():
        for var in vars_assigned:
            flowchart.edge(func, var, label="assigns", fontsize="10", color="blue")

    # Connect functions to global variables they use
    for func, vars_used in function_used_vars.items():
        for var in vars_used:
            if var in global_assigned_vars:
                flowchart.edge(var, func, label="uses", fontsize="10", color="purple")

    # Optionally, connect global variables to functions that assign them
    for var in global_assigned_vars:
        # If a global variable is assigned in the global scope, you might represent it differently
        pass  # Already represented as global variables; assignments are handled above

    logging.info("Function and variable flow diagram created successfully.")
    return flowchart


def parse_arguments() -> argparse.Namespace:
    """
    Parse command-line arguments.

    Returns:
        argparse.Namespace: The parsed arguments.
    """
    parser = argparse.ArgumentParser(
        description="Generate a function and variable flow diagram from a Python script."
    )

    # Optional argument for filepath with default value
    parser.add_argument(
        "-f",
        "--filepath",
        type=str,
        default=DEFAULT_FILEPATH,
        help=f"Path to the Python file to analyze. Default: {DEFAULT_FILEPATH}",
    )

    # Optional argument for output filename with default value
    parser.add_argument(
        "-o",
        "--output",
        type=str,
        default=DEFAULT_OUTPUT,
        help=f"Output filename for the diagram (without extension). Default: {DEFAULT_OUTPUT}",
    )

    # Optional verbose flag
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Enable verbose logging."
    )

    return parser.parse_args()


def main():
    # Parse command-line arguments
    args = parse_arguments()

    # Set logging level based on verbosity
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
        logging.debug("Verbose logging enabled.")

    # Extract function and variable flow and generate diagram
    diagram = extract_function_flow(args.filepath)
    try:
        diagram.render(args.output, cleanup=True)
        logging.info(f"Diagram saved as {args.output}.png")
    except Exception as e:
        logging.error(f"Failed to render the diagram: {e}")


if __name__ == "__main__":
    main()
