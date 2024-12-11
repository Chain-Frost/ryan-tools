# ryan_library.functions/tkinter_utils.py
import tkinter as tk
from tkinter.scrolledtext import ScrolledText
import queue
from typing import Generator
from loguru import logger


class TkinterApp:
    """A Tkinter application that manages multiple text widgets with thread-safe updates."""

    def __init__(self, title: str = "Processing", update_interval: int = 100) -> None:
        """Initialize the Tkinter application."""
        self.root = tk.Tk()
        self.root.title(title)
        self.update_interval = update_interval

        self.text_widgets: dict[str, ScrolledText] = {}  # Store text widgets
        self.line_counts: dict[str, int] = {}  # Line counts for each widget
        self.counters_enabled: dict[str, bool] = {}  # Manage counters' state
        self.title_bases: dict[str, str] = {}  # Base titles for widgets
        self.labels: dict[str, tk.Label] = {}  # References to the labels
        self.queue: queue.Queue[tuple[str, str]] = (
            queue.Queue()
        )  # Queue for text updates
        self.update_id: str | None = None  # ID for after method

    def create_text_box(
        self, title: str, row: int, column: int, height: int = 10, width: int = 100
    ) -> tuple[tk.Label, ScrolledText]:
        """Create a text box with a label and scrolled text widget."""
        frame = tk.Frame(self.root)
        frame.grid(row=row, column=column, sticky="nsew")
        label = tk.Label(frame, text=title)
        label.pack()
        text = ScrolledText(frame, wrap=tk.WORD, height=height, width=width)
        text.pack(fill=tk.BOTH, expand=True)
        return label, text

    def add_text_widget(
        self,
        key: str,
        title: str,
        row: int,
        column: int,
        *,
        height: int = 10,
        width: int = 100,
        counter: bool = False,
    ) -> None:
        """Add a text widget to the application."""
        if key not in self.text_widgets:
            label, text_widget = self.create_text_box(title, row, column, height, width)
            self.text_widgets[key] = text_widget
            self.line_counts[key] = 0
            self.counters_enabled[key] = counter
            self.title_bases[key] = title
            self.labels[key] = label

            if counter:
                self.update_widget_title(key)

    def append_text(self, key: str, text: str) -> None:
        """Append text to a widget in a thread-safe manner."""
        self.queue.put((key, text))

    def update_gui(self) -> None:
        """Process the queue and update the GUI."""
        try:
            while True:
                key, text = self.queue.get_nowait()
                if key in self.text_widgets and self.text_widgets[key].winfo_exists():
                    text_widget = self.text_widgets[key]
                    text_widget.insert(tk.END, text)
                    text_widget.see(tk.END)
                    self.line_counts[key] += 1

                    if self.counters_enabled[key]:
                        self.update_widget_title(key)
        except queue.Empty:
            pass
        except Exception as e:
            logger.error(f"Exception in update_gui: {e}")
        self.update_id = self.root.after(self.update_interval, self.update_gui)

    def update_widget_title(self, key: str) -> None:
        """Update the title of a widget to include the line count."""
        label = self.labels[key]
        label.config(text=f"{self.title_bases[key]} ({self.line_counts[key]})")

    def start_gui(self) -> None:
        """Start the Tkinter main loop."""
        self.update_gui()
        self.root.mainloop()

    def set_title(self, title: str) -> None:
        """Set the main window title."""
        self.root.title(title)

    def remove_text_widget(self, key: str) -> None:
        """Remove a text widget from the application."""
        if key in self.text_widgets:
            widget = self.text_widgets[key]
            widget.master.destroy()
            del self.text_widgets[key]
            del self.labels[key]
            del self.line_counts[key]
            del self.counters_enabled[key]
            del self.title_bases[key]

    def remove_all_text_widgets(self) -> None:
        """Remove all text widgets from the application."""
        keys = list(self.text_widgets.keys())
        for key in keys:
            self.remove_text_widget(key)

    def close(self) -> None:
        """Close the Tkinter application."""
        self.root.quit()


def grid_location_generator(
    start_row: int, start_column: int, max_columns: int
) -> Generator[tuple[int, int], None, None]:
    """Generate grid locations for placing widgets."""
    row = start_row
    column = start_column
    while True:
        yield (row, column)
        column += 1
        if column >= start_column + max_columns:
            column = start_column
            row += 1
