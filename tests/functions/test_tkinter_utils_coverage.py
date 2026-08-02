"""Tests for tkinter_utils.py."""

import pytest
from unittest.mock import MagicMock, patch

try:
    from ryan_library.functions.tkinter_utils import TkinterApp, grid_location_generator

    TKINTER_AVAILABLE = True
except ImportError:
    TKINTER_AVAILABLE = False


@pytest.mark.skipif(not TKINTER_AVAILABLE, reason="tkinter not available")
class TestTkinterUtils:
    @patch("ryan_library.functions.tkinter_utils.tk.Tk")
    @patch("ryan_library.functions.tkinter_utils.tk.Frame")
    @patch("ryan_library.functions.tkinter_utils.tk.Label")
    @patch("ryan_library.functions.tkinter_utils.ScrolledText")
    def test_tkinter_app_lifecycle(self, mock_scrolled, mock_label, mock_frame, mock_tk):
        mock_root = MagicMock()
        mock_tk.return_value = mock_root

        app = TkinterApp(title="Test", update_interval=10)
        assert app.update_interval == 10
        mock_root.title.assert_called_with("Test")

        # Add text widget
        app.add_text_widget("key1", "Title 1", 0, 0, counter=True)
        assert "key1" in app.text_widgets
        assert app.counters_enabled["key1"] is True

        # Append text
        app.append_text("key1", "hello\n")

        # Process queue
        app.update_gui()

        # Verify text was inserted
        mock_text = app.text_widgets["key1"]
        mock_text.insert.assert_called()

        # Update title (test coverage of set_title)
        app.set_title("New Title")
        mock_root.title.assert_called_with("New Title")

        # Remove widget
        app.remove_text_widget("key1")
        assert "key1" not in app.text_widgets

        # Remove all widgets (add another one first)
        app.add_text_widget("key2", "Title 2", 1, 0)
        app.remove_all_text_widgets()
        assert "key2" not in app.text_widgets

        # Start GUI
        app.start_gui()
        mock_root.mainloop.assert_called_once()

        # Close
        app.close()
        mock_root.quit.assert_called_once()

    def test_grid_location_generator(self):
        gen = grid_location_generator(0, 0, 2)
        assert next(gen) == (0, 0)
        assert next(gen) == (0, 1)
        assert next(gen) == (1, 0)
        assert next(gen) == (1, 1)
        assert next(gen) == (2, 0)
