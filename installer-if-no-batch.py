import sys
import subprocess
from pathlib import Path
import glob


def main() -> None:
    try:
        # Define the working path
        script_dir: Path = Path(__file__).parent.resolve()
        package_dir: Path = script_dir / "dist"
        print(f"PACKAGE_DIR: {package_dir}")

        if not package_dir.exists():
            print("Package directory does not exist.")
            input("Press Enter to exit.")
            sys.exit(1)

        # Find the latest version of the package
        package_pattern = str(package_dir / "ryan_functions-*.tar.gz")
        packages = sorted(glob.glob(package_pattern), reverse=True)

        if not packages:
            print("No package found in the directory.")
            input("Press Enter to exit.")
            sys.exit(1)

        latest_package = packages[0]
        print(f'Installing or updating "{Path(latest_package).name}"')

        # Install or update the package using pip
        result = subprocess.run(
            [sys.executable, "-m", "pip", "install", "--upgrade", latest_package]
        )

        # Check if the installation was successful
        if result.returncode == 0:
            print("Installation completed successfully.")
        else:
            print("Installation failed. Please check the path and try again.")

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        input("Press Enter to exit.")


if __name__ == "__main__":
    main()
