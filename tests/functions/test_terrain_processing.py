"""Tests for ryan_library.functions.terrain_processing."""

import numpy as np
import pandas as pd
import pytest
from unittest.mock import MagicMock, patch, call
from ryan_library.functions import terrain_processing

class TestTileData:
    def test_tile_data_exact_fit(self):
        """Test tiling when data fits exactly into tiles."""
        # Create 4x4 grid
        x = np.array([0, 1, 0, 1])
        y = np.array([0, 0, 1, 1])
        z = np.array([1, 2, 3, 4])
        df = pd.DataFrame({"X": x, "Y": y, "Z": z})
        
        # Tile size 1 should result in 4 tiles
        tiles = terrain_processing.tile_data(df, tile_size=1)
        assert len(tiles) == 4
        
        # Check indices
        indices = sorted([t[0] for t in tiles])
        assert indices == [(0, 0), (0, 1), (1, 0), (1, 1)]

    def test_tile_data_partial_fit(self):
        """Test tiling when data extends beyond tile boundaries."""
        x = np.array([0, 1.5])
        y = np.array([0, 0])
        z = np.array([1, 2])
        df = pd.DataFrame({"X": x, "Y": y, "Z": z})
        
        # Tile size 1. Data at 1.5 should be in second tile (index 1)
        tiles = terrain_processing.tile_data(df, tile_size=1)
        assert len(tiles) == 2
        
        indices = sorted([t[0] for t in tiles])
        assert indices == [(0, 0), (1, 0)]
        
        # Verify content
        t0 = next(t[1] for t in tiles if t[0] == (0, 0))
        assert len(t0) == 1
        assert t0.iloc[0]["X"] == 0
        
        t1 = next(t[1] for t in tiles if t[0] == (1, 0))
        assert len(t1) == 1
        assert t1.iloc[0]["X"] == 1.5

    def test_tile_data_empty(self):
        """Test tiling with empty DataFrame."""
        df = pd.DataFrame({"X": [], "Y": [], "Z": []})
        tiles = terrain_processing.tile_data(df, tile_size=10)
        assert len(tiles) == 0

class TestReadGeoTiff:
    @patch("rasterio.open")
    def test_read_geotiff_success(self, mock_open):
        """Test successful reading of GeoTIFF."""
        mock_ds = MagicMock()
        mock_open.return_value.__enter__.return_value = mock_ds
        
        # Mock data: 2x2 array
        mock_ds.read.return_value = np.array([[1, 2], [3, 4]])
        mock_ds.nodata = None
        
        # Mock xy coordinates
        # xy returns (x, y) for given (row, col)
        # We expect 4 calls or vectorized call
        # rasterio.io.DatasetReader.xy returns x, y arrays if inputs are arrays
        mock_ds.xy.return_value = (np.array([0, 1, 0, 1]), np.array([0, 0, 1, 1]))
        
        df = terrain_processing.read_geotiff("test.tif")
        
        assert len(df) == 4
        assert "X" in df.columns
        assert "Y" in df.columns
        assert "Z" in df.columns
        assert list(df["Z"]) == [1, 2, 3, 4]

    @patch("rasterio.open")
    def test_read_geotiff_nodata(self, mock_open):
        """Test reading with nodata masking."""
        mock_ds = MagicMock()
        mock_open.return_value.__enter__.return_value = mock_ds
        
        # Data with -9999 as nodata
        mock_ds.read.return_value = np.array([[1, -9999], [3, 4]])
        mock_ds.nodata = -9999
        
        # xy should be called only for valid pixels (3 pixels)
        mock_ds.xy.return_value = (np.array([0, 0, 1]), np.array([0, 1, 1]))
        
        df = terrain_processing.read_geotiff("test.tif")
        
        assert len(df) == 3
        assert -9999 not in df["Z"].values

    @patch("rasterio.open")
    def test_read_geotiff_error(self, mock_open):
        """Test error handling during read."""
        mock_open.side_effect = Exception("File not found")
        df = terrain_processing.read_geotiff("missing.tif")
        assert df.empty
        assert list(df.columns) == ["X", "Y", "Z"]

class TestProcessTerrainFile:
    @patch("ryan_library.functions.terrain_processing.read_geotiff")
    def test_process_terrain_file_inner_tiled(self, mock_read):
        """Test processing with tiling."""
        df = pd.DataFrame({
            "X": [0, 10],
            "Y": [0, 0],
            "Z": [1, 2]
        })
        mock_read.return_value = df
        
        save_mock = MagicMock()
        
        terrain_processing.process_terrain_file_inner(
            "test.tif", "out_dir", None, tile_size=5, save_function=save_mock
        )
        
        # Should result in 2 tiles (0,0) and (2,0) if tile_size=5?
        # 0 to 5 is tile 0. 10 is in tile 2 (10 // 5 = 2).
        assert save_mock.call_count == 2
        
    @patch("ryan_library.functions.terrain_processing.read_geotiff")
    def test_process_terrain_file_inner_no_tile(self, mock_read):
        """Test processing without tiling."""
        df = pd.DataFrame({"X": [0], "Y": [0], "Z": [1]})
        mock_read.return_value = df
        
        save_mock = MagicMock()
        
        terrain_processing.process_terrain_file_inner(
            "test.tif", "out_dir", None, tile_size=None, save_function=save_mock
        )
        
        assert save_mock.call_count == 1
        args = save_mock.call_args[0]
        assert args[1] == "out_dir"
        assert args[2] == "test" # base filename

    @patch("ryan_library.functions.terrain_processing.read_geotiff")
    def test_process_terrain_file_inner_empty(self, mock_read):
        """Test processing empty file."""
        mock_read.return_value = pd.DataFrame(columns=["X", "Y", "Z"])
        save_mock = MagicMock()
        
        terrain_processing.process_terrain_file_inner(
            "test.tif", "out_dir", None, tile_size=10, save_function=save_mock
        )
        
        save_mock.assert_not_called()

class TestParallelProcessing:
    @patch("ryan_library.functions.terrain_processing.Pool")
    @patch("ryan_library.functions.terrain_processing.cpu_count")
    def test_parallel_process_multiple_terrain(self, mock_cpu, mock_pool):
        """Test parallel processing orchestration."""
        mock_cpu.return_value = 2
        pool_instance = mock_pool.return_value.__enter__.return_value
        pool_instance.imap_unordered.return_value = []
        
        files = ["f1.tif", "f2.tif"]
        save_mock = MagicMock()
        
        terrain_processing.parallel_process_multiple_terrain(
            files, "out", None, 10, save_mock
        )
        
        assert pool_instance.imap_unordered.called
        args = pool_instance.imap_unordered.call_args[0]
        func = args[0]
        tasks = args[1]
        
        assert func == terrain_processing.process_terrain_file
        assert len(tasks) == 2
