import ast
from typing import List, Tuple
from openpyxl import Workbook

def extract_script_structure(file_path: str) -> List[Tuple[str, str, str]]:
    """
    Extract the structure of a Python script.
    Args:
        file_path (str): Path to the Python script file.
    Returns:
        List[Tuple[str, str, str]]: List of structure elements (Type, Name, Parent).
    """
    structure = []

    class ScriptVisitor(ast.NodeVisitor):
        def __init__(self):
            self.current_parent = None

        def visit_Module(self, node):
            self.current_parent = "Module"
            structure.append(("Module", "Main", "None"))
            self.generic_visit(node)

        def visit_ClassDef(self, node):
            class_name = node.name
            structure.append(("Class", class_name, self.current_parent))
            previous_parent = self.current_parent
            self.current_parent = class_name
            self.generic_visit(node)
            self.current_parent = previous_parent

        def visit_FunctionDef(self, node):
            function_name = node.name
            structure.append(("Function", function_name, self.current_parent))
            self.generic_visit(node)

    with open(file_path, "r") as file:
        tree = ast.parse(file.read(), filename=file_path)

    visitor = ScriptVisitor()
    visitor.visit(tree)
    return structure

def save_structure_to_excel(structure: List[Tuple[str, str, str]], output_file: str):
    """
    Save the script structure to an Excel file.
    Args:
        structure (List[Tuple[str, str, str]]): Script structure.
        output_file (str): Path to the output Excel file.
    """
    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "Script Structure"

    # Write headers
    sheet.append(["Type", "Name", "Parent"])

    # Write structure rows
    for row in structure:
        sheet.append(row)

    # Save workbook
    workbook.save(output_file)

# Example Usage
python_file = r"Q:\Library\Automation\ryan-tools\ryan-scripts\TUFLOW-python\TUFLOW_SimpleCulvert_v13.py"  # Replace with the path to your Python script
output_excel = "script_structure.xlsx"  # Output Excel file

structure = extract_script_structure(python_file)
save_structure_to_excel(structure, output_excel)

print(f"Script structure exported to {output_excel}.")
