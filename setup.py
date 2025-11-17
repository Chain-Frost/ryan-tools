# setup.py
from setuptools import setup, find_packages  # type: ignore

setup(
    name="ryan_functions",
    # Version scheme: yy.mm.dd.release_number
    # Increment when publishing new wheels
    version="25.11.17.4",
    packages=find_packages(exclude=["*.tests", "*.tests.*", "tests.*", "tests"]),
    include_package_data=True,  # Include package data as specified in MANIFEST.in
    # package_data={"ryan_library": ["py.typed"]},
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
        "pyright",
        "laspy",
        "tqdm",
        "rasterio",
        "seaborn",
        "requests",
        "openpyxl",
        "loguru",
        "tabulate",
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
