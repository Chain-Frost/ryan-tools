# setup.py
from setuptools import find_namespace_packages, find_packages, setup  # type: ignore

PACKAGE_EXCLUDES: list[str] = ["*.tests", "*.tests.*", "tests.*", "tests"]
VENDORED_NAMESPACE_EXCLUDES: list[str] = [
    "vendor.run_hy8.dist",
    "vendor.run_hy8.dist.*",
    "vendor.run_hy8.docs",
    "vendor.run_hy8.docs.*",
    "vendor.run_hy8.scripts",
    "vendor.run_hy8.scripts.*",
    "vendor.run_hy8.tests",
    "vendor.run_hy8.tests.*",
]
packages: list[str] = find_packages(exclude=PACKAGE_EXCLUDES)
packages.extend(
    find_namespace_packages(
        include=["vendor.run_hy8", "vendor.run_hy8.*"],
        exclude=VENDORED_NAMESPACE_EXCLUDES,
    )
)

setup(
    name="ryan_functions",
    # Version scheme: yy.mm.dd.release_number
    # Increment when publishing new wheels
    version="26.03.05.3",
    packages=packages,
    include_package_data=True,  # Include package data as specified in MANIFEST.in
    # package_data={"ryan_library": ["py.typed"]},
    install_requires=[
        "numpy",
        "pandas",
        "pyshp",
        "geopandas",
        "fiona",
        "pyarrow",
        "fastparquet",
        "matplotlib",
        "colorama",
        "XlsxWriter",
        "psutil",
        "black",
        "pyright",
        "laspy",
        "tqdm",
        "rasterio",
        "seaborn",
        "requests",
        "openpyxl",
        "loguru",
        "tabulate",
        "pypdf",
        # "beautifulsoup4",
        # Add any dependencies here, e.g., 'numpy', 'pandas'
    ],
    author="Chain Frost",
    author_email="chainfrost@outlook.com",
    description="Collection of TUFLOW and data processing functions.",
    long_description=open("README.md").read(),  # Assumes a README.md file is present
    long_description_content_type="text/markdown",
    url="https://github.com/Chain-Frost/ryan-tools",
    classifiers=[
        "Programming Language :: Python :: 3.13",
        "Operating System :: Windows",
        # 'License :: OSI Approved :: MIT License',  # Example license
    ],
    python_requires=">=3.12",
)
