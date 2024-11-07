import subprocess
import sys

def install_jupyter():
    """Installs Jupyter using pip via python.exe."""
    try:
        # Run the pip install command
        subprocess.check_call([sys.executable, "-m", "pip", "install", "jupyter"])
        print("Jupyter installed successfully.")
    except subprocess.CalledProcessError:
        print("An error occurred while installing Jupyter.")

if __name__ == "__main__":
    install_jupyter()
