"""Tests for ryan_library.functions.tkinter_utils."""

import pytest
from unittest.mock import MagicMock, patch, call
import queue
import tkinter as tk

# Mock tkinter before importing the module if possible, but since it's imported at top level,
# we might need to patch sys.modules or rely on patching inside the test.
# However, the module has a try/except block. If we are in a headless env, it might fail to import tkinter.
# Assuming tkinter is available or we can mock it.

from ryan_library.functions import tkinter_utils

class TestTkinterApp:
    @pytest.fixture
    def mock_tk(self):
        with patch("ryan_library.functions.tkinter_utils.tk") as mock:
            # Setup mock root
            mock.Tk.return_value = MagicMock()
            yield mock

    @pytest.fixture
    def mock_scrolled_text(self):
        with patch("ryan_library.functions.tkinter_utils.ScrolledText") as mock:
            yield mock

    def test_init(self, mock_tk):
        app = tkinter_utils.TkinterApp("Title")
        mock_tk.Tk.assert_called()
        app.root.title.assert_called_with("Title")
        assert app.text_widgets == {}

    def test_add_text_widget(self, mock_tk, mock_scrolled_text):
        app = tkinter_utils.TkinterApp()
        app.add_text_widget("key1", "Widget 1", 0, 0)
        
        assert "key1" in app.text_widgets
        assert "key1" in app.labels
        mock_tk.Label.assert_called()
        mock_scrolled_text.assert_called()

    def test_append_text(self, mock_tk):
        app = tkinter_utils.TkinterApp()
        app.append_text("key1", "hello")
        assert app.queue.get() == ("key1", "hello")

    def test_update_gui(self, mock_tk, mock_scrolled_text):
        app = tkinter_utils.TkinterApp()
        # Setup widget
        app.add_text_widget("key1", "Title", 0, 0)
        widget_mock = app.text_widgets["key1"]
        widget_mock.winfo_exists.return_value = True
        
        # Add to queue
        app.queue.put(("key1", "line1\n"))
        
        # Mock root.after to prevent actual scheduling (though it's a mock object anyway)
        app.root.after = MagicMock()
        
        app.update_gui()
        
        widget_mock.insert.assert_called_with(mock_tk.END, "line1\n")
        app.root.after.assert_called()

    def test_update_widget_title(self, mock_tk, mock_scrolled_text):
        app = tkinter_utils.TkinterApp()
        app.add_text_widget("key1", "Title", 0, 0, counter=True)
        
        app.line_counts["key1"] = 5
        app.update_widget_title("key1")
        
        label_mock = app.labels["key1"]
        label_mock.config.assert_called_with(text="Title (5)")

    def test_remove_text_widget(self, mock_tk, mock_scrolled_text):
        app = tkinter_utils.TkinterApp()
        app.add_text_widget("key1", "Title", 0, 0)
        
        app.remove_text_widget("key1")
        assert "key1" not in app.text_widgets

    def test_grid_location_generator(self):
        gen = tkinter_utils.grid_location_generator(0, 0, 2)
        assert next(gen) == (0, 0)
        assert next(gen) == (0, 1)
        assert next(gen) == (1, 0)
        assert next(gen) == (1, 1)
