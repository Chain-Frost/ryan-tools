# ryan_library/processors/tuflow/maximums_1d/ccAProcessor.py

__lazy_modules__ = ["pandas"]
import shutil
import sqlite3
import tempfile
import urllib.parse
from contextlib import ExitStack
from pathlib import Path
from typing import ClassVar

import pandas as pd
import shapefile  # pyright: ignore[reportMissingTypeStubs]
from loguru import logger
from pandas import DataFrame

from ..base_processor import BaseProcessor


class ccAProcessor(BaseProcessor):
    """Processor for CCA files ('_1d_ccA_L.dbf' and '_Results1D.gpkg')."""

    # Common renames across both DBF and GPKG sources
    _COLUMN_RENAMES: ClassVar[dict[str, str]] = {"Channel": "Chan ID"}
    # Mapping of shapefile-safe field names (max 10 chars) to canonical names
    _SHAPEFILE_COLUMN_RENAMES: ClassVar[dict[str, str]] = {"Dur_10pFul": "Dur_10pFull"}

    def process(self) -> None:
        """Process the CCA file (DBF or GPKG) and return a cleaned DataFrame.

        Sets:
            pd.DataFrame: Processed CCA data.
        """
        logger.debug("Starting processing of CCA file: {}", self.log_path)
        file_ext = self.file_path.suffix.lower()

        try:
            if file_ext == ".dbf":
                cca_data: pd.DataFrame = self.process_dbf()
            elif file_ext == ".gpkg":
                cca_data = self.process_gpkg()
            else:
                logger.error(f"Unsupported CCA file type: {self.log_path}")
                self.df = pd.DataFrame()
                return

            if cca_data.empty:
                logger.error(f"No data read from CCA file: {self.log_path}")
                self.df = pd.DataFrame()
                return

            if "Chan ID" not in cca_data.columns:
                logger.error("{}: ccA data is missing the required channel identifier", self.file_name)
                self.df = pd.DataFrame()
                return

            self.df = cca_data

            self.apply_entity_filter()
            # Add common columns like run codes, paths, etc.
            self.add_common_columns()

            # Apply output transformations (data types) as per config
            self.apply_output_transformations()

            # Validate data
            if not self.validate_data():
                logger.error(f"{self.file_name}: Data validation failed.")
                self.df = pd.DataFrame()
                return

            self.processed = True
            logger.info(f"Completed processing of CCA file: {self.log_path}")
            return
        except Exception as e:
            logger.error(f"Failed to process CCA file {self.log_path}: {e}")
            self.df = pd.DataFrame()
            return

    def _normalize_column_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """Rename truncated or variant columns so downstream config matches."""
        rename_map: dict[str, str] = {}
        for source, target in self._COLUMN_RENAMES.items():
            if source in df.columns and target not in df.columns:
                rename_map[source] = target
        for truncated, full in self._SHAPEFILE_COLUMN_RENAMES.items():
            if truncated in df.columns and full not in df.columns:
                rename_map[truncated] = full
        if rename_map:
            logger.debug("{}: Renaming columns for consistency: {}", self.file_name, rename_map)
            df = df.rename(columns=rename_map)
        return df

    def process_dbf(self) -> pd.DataFrame:
        """Process a DBF CCA file.

        Returns:
            pd.DataFrame: Processed DBF CCA data.
        """
        logger.debug("Processing DBF CCA file: {}", self.log_path)
        try:
            with self.file_path.open("rb") as dbf_file:
                sf = shapefile.Reader(dbf=dbf_file)
                records = sf.records()
                fieldnames: list[str] = [field[0] for field in sf.fields[1:]]
                cca_data = pd.DataFrame(records, columns=fieldnames)
                # Normalise column names so downstream configuration and validation align
                cca_data: DataFrame = self._normalize_column_names(df=cca_data)
            logger.debug("Processed DBF CCA DataFrame head:\n{}", cca_data.head())
            return cca_data
        except Exception as e:
            logger.error(f"Error processing DBF file {self.log_path}: {e}")
            return pd.DataFrame()

    @staticmethod
    def _build_sqlite_connection_targets(path: Path) -> list[tuple[str, str]]:
        """Construct read-only URI alternatives, including UNC paths, without a writable fallback."""
        posix_path = path.absolute().as_posix()
        encoded_path = urllib.parse.quote(posix_path, safe="/:")
        targets = [("uri_standard", f"file:{encoded_path}?mode=ro")]

        if posix_path.startswith("//"):
            # SQLite rejects remote URI authorities on some Windows builds. The four-slash
            # form retains the UNC double-leading-slash path rather than changing its target.
            targets.append(("uri_unc_path", f"file://{encoded_path}?mode=ro"))

        return targets

    @staticmethod
    def _source_signature(path: Path) -> tuple[int, int, int, int] | None:
        """Return physical identity and content-change metadata without writing."""
        try:
            stat = path.stat()
        except FileNotFoundError:
            return None
        return stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns

    def _prepare_gpkg_read_source(self, path: Path, owned: ExitStack) -> Path | None:
        """Keep WAL-mode SQLite opens on owned local copies, never on source sidecars.

        A WAL writer's final checkpoint can remove -wal/-shm after any existence
        check. SQLite mode=ro can then recreate those files in a writable source
        directory. Copy only WAL-mode sources to local scratch so SQLite's own
        sidecar/lock operations stay invocation-owned. Never use immutable=1:
        it can silently miss committed frames from an active writer.
        """
        try:
            with path.open("rb") as database_file:
                header = database_file.read(20)
            if not (header.startswith(b"SQLite format 3\x00") and header[18:20] == b"\x02\x02"):
                return path

            wal = path.with_name(path.name + "-wal")
            shm = path.with_name(path.name + "-shm")
            source_paths = (path, wal, shm)
            before = tuple(self._source_signature(item) for item in source_paths)
            if (before[1] is None) != (before[2] is None):
                logger.error("process_gpkg: Incomplete WAL/SHM sidecars for {}; refusing read", path)
                return None

            scratch = Path(owned.enter_context(tempfile.TemporaryDirectory(prefix="ryan-cca-wal-")))
            staged_path = scratch / path.name
            shutil.copyfile(path, staged_path)
            # SQLite reconstructs -shm in scratch; copying a live shared index is
            # unnecessary and may fail on Windows when the writer holds its lock.
            if before[1] is not None:
                shutil.copyfile(wal, scratch / wal.name)
            if tuple(self._source_signature(item) for item in source_paths) != before:
                logger.error("process_gpkg: Source changed while copying WAL snapshot of {}; refusing read", path)
                return None
            logger.debug("process_gpkg: Reading WAL-mode {} from owned local snapshot", path)
            return staged_path
        except OSError as exc:
            logger.error("process_gpkg: Could not prepare read-only snapshot of {}: {}", path, exc)
            return None

    def _open_sqlite_connection(self, path: Path) -> sqlite3.Connection | None:
        """Open a rollback-mode source or owned WAL snapshot, without write fallback."""
        errors: list[str] = []
        for label, target in self._build_sqlite_connection_targets(path):
            conn: sqlite3.Connection | None = None
            try:
                conn = sqlite3.connect(database=target, uri=True, timeout=0.5)
                conn.execute("PRAGMA query_only=ON")
                logger.debug("process_gpkg: Opened {} using read-only strategy {}", path, label)
                return conn
            except sqlite3.Error as exc:
                if conn is not None:
                    conn.close()
                errors.append(f"{label}: {exc}")

        logger.error("process_gpkg: Unable to open {} read-only: {}", path, " | ".join(errors))
        return None

    @staticmethod
    def _quote_identifier(value: str) -> str:
        """Quote a schema-provided SQLite identifier, including embedded quotes."""
        return '"' + value.replace('"', '""') + '"'

    def process_gpkg(self) -> pd.DataFrame:
        """Read one unambiguous ccA layer from a consistent committed, read-only SQLite snapshot."""
        path = self.file_path
        if not path.is_file():
            logger.error("process_gpkg: GeoPackage does not exist or is not a file: {}", path)
            return pd.DataFrame()

        owned = ExitStack()
        read_path = self._prepare_gpkg_read_source(path, owned)
        if read_path is None:
            owned.close()
            return pd.DataFrame()
        conn = self._open_sqlite_connection(read_path)
        if conn is None:
            owned.close()
            return pd.DataFrame()

        try:
            # A deferred read transaction pins one committed snapshot across the layer,
            # geometry and attribute queries, including commits still held in the WAL.
            conn.execute("BEGIN")
            layers = [
                row[0]
                for row in conn.execute(
                    "SELECT table_name FROM gpkg_contents "
                    "WHERE data_type = 'features' ORDER BY table_name COLLATE BINARY"
                ).fetchall()
            ]
            matches = [name for name in layers if isinstance(name, str) and name.endswith("1d_ccA_L")]
            if len(matches) != 1:
                logger.error(
                    "process_gpkg: Expected exactly one ccA feature layer in {}, found {}: {}",
                    path,
                    len(matches),
                    matches,
                )
                return pd.DataFrame()
            layer_name = matches[0]

            geometry_rows = conn.execute(
                "SELECT column_name FROM gpkg_geometry_columns WHERE table_name = ?", (layer_name,)
            ).fetchall()
            if len(geometry_rows) != 1 or not isinstance(geometry_rows[0][0], str):
                logger.error(
                    "process_gpkg: Missing or ambiguous geometry-column metadata for {} in {}", layer_name, path
                )
                return pd.DataFrame()
            geometry_column: str = geometry_rows[0][0]

            table_info = conn.execute(f"PRAGMA table_info({self._quote_identifier(layer_name)})").fetchall()
            if not table_info or geometry_column not in {row[1] for row in table_info}:
                logger.error("process_gpkg: Invalid geometry-column metadata for {} in {}", layer_name, path)
                return pd.DataFrame()

            # Only SQLite's INTEGER PRIMARY KEY is a row identifier. Keep other primary
            # keys and ordinary fields (including any column named 'geom') unless they
            # are actually the registered GeoPackage geometry column.
            value_cols: list[str] = [
                row[1]
                for row in table_info
                if row[1] != geometry_column and not (row[5] and str(row[2]).upper() == "INTEGER")
            ]
            if not value_cols:
                logger.error("process_gpkg: No ccA attribute columns in {} ({})", path, layer_name)
                return pd.DataFrame()

            columns_sql = ", ".join(self._quote_identifier(col) for col in value_cols)
            select_sql = f"SELECT {columns_sql} FROM {self._quote_identifier(layer_name)}"  # noqa: S608
            cur = conn.execute(select_sql)
            cca_data = pd.DataFrame(cur.fetchall(), columns=[column[0] for column in cur.description])
        except sqlite3.Error as exc:
            logger.error("process_gpkg: Unable to read consistent ccA snapshot from {}: {}", path, exc)
            return pd.DataFrame()
        finally:
            try:
                conn.close()
            finally:
                owned.close()

        cca_data = self._normalize_column_names(cca_data)
        if "Chan ID" not in cca_data.columns:
            logger.error("process_gpkg: ccA layer {} in {} lacks required channel identifier", layer_name, path)
            return pd.DataFrame()

        # The other output_columns are optional, just as for the DBF path: the
        # configured dtype conversion applies to whatever attributes are present.
        logger.debug("process_gpkg: Read {} ccA records from {}", len(cca_data), path)
        return cca_data
