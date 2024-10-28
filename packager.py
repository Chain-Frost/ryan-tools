import os
import sys
import subprocess
from pathlib import Path
import shutil
import glob

def main():
    try:
        # Define the directory where the package will be stored
        script_dir = Path(__file__).parent.resolve()
        package_dir = script_dir / "dist"

        # Navigate to the directory containing the setup.py script
        os.chdir(script_dir)
        print(f"Changed directory to {script_dir}")

        # Clean previous builds
        if package_dir.exists():
            print(f"Removing existing package directory: {package_dir}")
            shutil.rmtree(package_dir)

        # Create the source distribution
        print("Creating source distribution...")
        result = subprocess.run([sys.executable, "setup.py", "sdist"])

        # Check if the build was successful
        if result.returncode != 0:
            print("Build failed. Please check the setup.py for errors.")
            input("Press Enter to exit.")
            sys.exit(1)

        # Ensure the package directory exists after build
        if not package_dir.exists():
            package_dir.mkdir(parents=True, exist_ok=True)

        # Move the generated tar.gz file to the package directory
        tar_files = glob.glob(str(script_dir / "dist" / "*.tar.gz"))
        if not tar_files:
            print("No tar.gz file found after build.")
            input("Press Enter to exit.")
            sys.exit(1)

        for tar_file in tar_files:
            shutil.move(tar_file, package_dir)
            print(f"Moved {Path(tar_file).name} to {package_dir}")

        print(f"Package created and moved to {package_dir} successfully.")

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        input("Press Enter to exit.")

if __name__ == "__main__":
    main()
