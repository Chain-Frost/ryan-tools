# ryan_library/functions/gdal/gdal_environment.py

import os
from pathlib import Path
from loguru import logger


def find_qgis_install_path() -> Path:
    """
    Find the QGIS or OSGeo4W installation path.

    Returns:
        Path: The root path of the QGIS/OSGeo4W installation.

    Raises:
        FileNotFoundError: If the installation path cannot be found.
    """
    # Priority: Check for OSGeo4W environment setup script first
    osgeo_env_path = Path("C:/OSGeo4W/bin/o4w_env.bat")
    if osgeo_env_path.exists():
        logger.info(f"Found OSGeo4W environment setup script at: {osgeo_env_path}")
        return osgeo_env_path.parent.parent  # C:/OSGeo4W

    # Search for the latest QGIS directory in "C:/Program Files"
    program_files = Path("C:/Program Files")
    qgis_dirs = sorted(program_files.glob("QGIS*"), reverse=True)  # Latest first

    for qgis_dir in qgis_dirs:
        env_script = qgis_dir / "bin" / "o4w_env.bat"
        if env_script.exists():
            logger.info(f"Found QGIS environment setup script at: {env_script}")
            return qgis_dir

    # If not found, raise an error
    raise FileNotFoundError("QGIS or OSGeo4W installation not found.")


def find_python_installation(qgis_path: Path) -> Path:
    """
    Find the Python installation directory within the QGIS/OSGeo4W installation.

    Args:
        qgis_path (Path): The root path of the QGIS/OSGeo4W installation.

    Returns:
        Path: The Python installation directory.

    Raises:
        FileNotFoundError: If the Python installation directory cannot be found.
    """
    python_pattern = "Python3*"
    python_dirs = sorted(qgis_path.glob(f"apps/{python_pattern}"), reverse=True)  # Latest first

    if not python_dirs:
        logger.error("Python installation not found within QGIS/OSGeo4W apps directory.")
        raise FileNotFoundError("Python installation not found within QGIS/OSGeo4W apps directory.")

    python_dir = python_dirs[0]  # Choose the latest Python version
    logger.info(f"Detected Python installation at: {python_dir}")
    return python_dir


def setup_environment(qgis_path: Path = None):
    """
    Set up the environment variables needed for GDAL processing based on the QGIS/OSGeo4W installation path.

    Args:
        qgis_path (Path, optional): Custom QGIS/OSGeo4W installation path. Defaults to None.

    Raises:
        FileNotFoundError: If required executables are not found.
    """
    if not qgis_path:
        qgis_path = find_qgis_install_path()

    logger.info(f"Using QGIS/OSGeo4W installation at: {qgis_path}")

    # Find the Python installation directory dynamically
    python_dir = find_python_installation(qgis_path)

    # Set environment variables
    os.environ["OSGEO4W_ROOT"] = str(qgis_path)
    os.environ["GDAL_DATA"] = str(qgis_path / "apps" / "gdal" / "share" / "gdal")
    os.environ["GDAL_DRIVER_PATH"] = str(qgis_path / "apps" / "gdal" / "lib" / "gdalplugins")
    os.environ["GS_LIB"] = str(qgis_path / "apps" / "gs" / "lib")
    os.environ["OPENSSL_ENGINES"] = str(qgis_path / "lib" / "engines-3")
    os.environ["SSL_CERT_FILE"] = str(qgis_path / "bin" / "curl-ca-bundle.crt")
    os.environ["SSL_CERT_DIR"] = str(qgis_path / "apps" / "openssl" / "certs")
    os.environ["PDAL_DRIVER_PATH"] = str(qgis_path / "apps" / "pdal" / "plugins")
    os.environ["PROJ_DATA"] = str(qgis_path / "share" / "proj")
    os.environ["PYTHONHOME"] = str(python_dir)
    os.environ["PYTHONUTF8"] = "1"
    os.environ["QT_PLUGIN_PATH"] = str(qgis_path / "apps" / "Qt5" / "plugins")
    os.environ["PATH"] = f"{python_dir / 'Scripts'};" f"{qgis_path / 'bin'};" f"{os.environ['PATH']}"

    logger.debug("Environment variables set successfully.")

    # Define GDAL tool paths
    gdal_calc_path = python_dir / "Scripts" / "gdal_calc.py"
    gdal_polygonize_path = python_dir / "Scripts" / "gdal_polygonize.py"
    gdal_translate_path = qgis_path / "bin" / "gdal_translate.exe"

    # Check executables
    check_executable(str(gdal_translate_path), "gdal_translate")
    check_executable(str(gdal_calc_path), "gdal_calc.py")
    check_executable(str(gdal_polygonize_path), "gdal_polygonize.py")

    # Set GDAL tool paths in environment variables for easy access
    os.environ["GDAL_CALC_PATH"] = str(gdal_calc_path)
    os.environ["GDAL_POLYGONIZE_PATH"] = str(gdal_polygonize_path)
    os.environ["GDAL_TRANSLATE_PATH"] = str(gdal_translate_path)

    logger.debug("GDAL tool paths set in environment variables.")


def check_executable(path: str, name: str):
    """
    Check if the specified executable or script exists.

    Args:
        path (str): Path to the executable or script.
        name (str): Name of the executable or script.

    Raises:
        FileNotFoundError: If the executable or script is not found.
    """
    if not Path(path).exists():
        logger.error(f"Error: {name} not found at {path}. Ensure it is correctly installed.")
        raise FileNotFoundError(f"{name} not found at {path}.")


def check_required_components():
    """
    Check that all required GDAL components are available.

    Raises:
        FileNotFoundError: If any required GDAL component is not found.
    """
    # Retrieve GDAL tool paths from environment variables
    gdal_calc_path = Path(os.environ.get("GDAL_CALC_PATH", ""))
    gdal_polygonize_path = Path(os.environ.get("GDAL_POLYGONIZE_PATH", ""))
    gdal_translate_path = Path(os.environ.get("GDAL_TRANSLATE_PATH", ""))

    # Check executables
    check_executable(str(gdal_translate_path), "gdal_translate")
    check_executable(str(gdal_calc_path), "gdal_calc.py")
    check_executable(str(gdal_polygonize_path), "gdal_polygonize.py")

    logger.debug("All required GDAL components are available.")
