@echo off
REM ============================================================================
REM Lists all files in the current directory (non-recursive) and saves to a text file.
REM
REM /b    : Bare format (no heading info or summary)
REM /o:en : Order by Extension, then by Name
REM
REM NOTE: If you need to target a network path but are blocked from running
REM       batch scripts there, use the recursive Python equivalent
REM       (and remove the recursive glob if needed):
REM       ryan-scripts/unsorted-python/list_files_recursive.py
REM ============================================================================

dir /b /o:en > filenames.txt
