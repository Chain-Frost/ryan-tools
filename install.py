import os
import sys
import subprocess
from pathlib import Path
import glob
import shutil
from setuptools import setup, find_packages


def create_manifest(script_dir):
    """
    Creates a MANIFEST.in file to exclude .egg-info directories from the source distribution.
    """
    manifest_path = script_dir / "MANIFEST.in"
    with open(manifest_path, "w") as f:
        f.write("global-exclude *.egg-info\n")
        f.write("prune *.egg-info\n")
    print("Created MANIFEST.in to exclude .egg-info directories.")


def clean_build_directories(script_dir):
    """
    Removes existing build directories and .egg-info directories to ensure a clean build.
    """
    build_dirs = ["build", "dist"]
    for dir_name in build_dirs:
        dir_path = script_dir / dir_name
        if dir_path.exists():
            shutil.rmtree(dir_path)
            print(f"Removed existing directory: {dir_path}")

    # Remove any .egg-info directories at the root
    egg_info_dirs = list(script_dir.glob("*.egg-info"))
    for egg_dir in egg_info_dirs:
        shutil.rmtree(egg_dir)
        print(f"Removed existing .egg-info directory: {egg_dir}")


def build_sdist_directly(script_dir):
    """
    Builds the source distribution by directly invoking setup().
    """
    print("Building source distribution directly using setup()...")
    # Temporarily override sys.argv to simulate command-line invocation
    original_argv = sys.argv.copy()
    sys.argv = [sys.argv[0], "sdist"]

    try:
        setup(
            name="ryan_functions",
            version="0.23",
            packages=find_packages(
                exclude=["*.tests", "*.tests.*", "tests.*", "tests"]
            ),
            install_requires=[
                "numpy",
                "pandas",
                "pyshp",
                "geopandas",
                "fiona",
                "pyarrow",
                "matplotlib",
                "colorama",
                "XlsxWriter",
                "psutil",
                "black",
                "mypy",
                "colorlog",
                # Add any additional dependencies here
            ],
            author="Chain Frost",
            author_email="chainfrost@outlook.com",
            description="Collection of TUFLOW and data processing functions.",
            long_description=open("README.md").read(),  # Ensure README.md exists
            long_description_content_type="text/markdown",
            url="https://github.com/Chain-Frost/ryan-tools",
            classifiers=[
                "Programming Language :: Python :: 3",
                "Operating System :: Windows",
                # Uncomment and modify the following line if you have a license
                # "License :: OSI Approved :: MIT License",
            ],
            python_requires=">=3.12",
        )
    except SystemExit as e:
        if e.code != 0:
            print("Build failed. Please check the setup configuration for errors.")
            sys.exit(1)
    finally:
        # Restore the original sys.argv
        sys.argv = original_argv
    print("Source distribution built successfully.")


def verify_build(package_dir):
    """
    Verifies that the sdist was created successfully.
    """
    tar_files = sorted(package_dir.glob("ryan_functions-*.tar.gz"), reverse=True)
    if not tar_files:
        print("Failed to create the source distribution. No .tar.gz file found.")
        sys.exit(1)

    print(f"Package created successfully in {package_dir}:")
    for tar in tar_files:
        print(f" - {tar.name}")
    return tar_files[0]


def install_package(latest_package):
    """
    Installs or updates the package using pip.
    """
    print(f'Installing or updating "{latest_package.name}"')
    result = subprocess.run(
        [sys.executable, "-m", "pip", "install", "--upgrade", str(latest_package)]
    )

    # Check if the installation was successful
    if result.returncode == 0:
        print("Installation completed successfully.")
    else:
        print("Installation failed. Please check the path and try again.")
        sys.exit(1)


def main():
    try:
        # Define the working path
        script_dir = Path(__file__).parent.resolve()
        package_dir = script_dir / "dist"
        print(f"PACKAGE_DIR: {package_dir}")

        # Step 1: Clean previous builds and .egg-info directories
        clean_build_directories(script_dir)

        # Step 2: Create MANIFEST.in to exclude .egg-info
        create_manifest(script_dir)

        # Step 3: Build the source distribution directly
        build_sdist_directly(script_dir)

        # Step 4: Verify the build
        latest_package = verify_build(package_dir)

        # Step 5: Install or update the package
        install_package(latest_package)

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        input("Press Enter to exit.")


if __name__ == "__main__":
    main()
