from __future__ import annotations

import ctypes
import shutil
from ctypes import wintypes
from pathlib import Path


source = Path(r"C:\path\file.zip")
share = r"\\ipv4\folder"
destination = Path(share) / source.name
username = r"user"
password = "pass"

if not source.is_file():
    raise FileNotFoundError(f"Source file not found: {source}")

RESOURCETYPE_DISK = 0x00000001
NO_ERROR = 0
ERROR_SESSION_CREDENTIAL_CONFLICT = 1219

class NETRESOURCE(ctypes.Structure):
    _fields_= [
        ("dwScope", wintypes.DWORD),
        ("dwType", wintypes.DWORD),
        ("dwDisplayType", wintypes.DWORD),
        ("dwUsage", wintypes.DWORD),
        ("lpLocalName", wintypes.LPWSTR),
        ("lpRemoteName", wintypes.LPWSTR),
        ("lpComment", wintypes.LPWSTR),
        ("lpProvider", wintypes.LPWSTR),
    ]

mpr = ctypes.WinDLL("mpr")
mpr.WNetAddConnection2W.argtypes = [ctypes.POINTER(NETRESOURCE), wintypes.LPCWSTR, wintypes.LPCWSTR, wintypes.DWORD]
mpr.WNetAddConnection2W.restype = wintypes.DWORD
mpr.WNetCancelConnection2W.argtypes = [wintypes.LPCWSTR, wintypes.DWORD, wintypes.BOOL]
mpr.WNetCancelConnection2W.restype = wintypes.DWORD

nr = NETRESOURCE()
nr.dwType = RESOURCETYPE_DISK
nr.lpRemoteName = share

connected_here = False
result = mpr.WNetAddConnection2W(ctypes.byref(nr), password, username, 0)
if result == NO_ERROR:
    connected_here = True
elif result == ERROR_SESSION_CREDENTIAL_CONFLICT:
    # A connection already exists for this server/share; try using it.
    pass
else:
    raise ctypes.WinError(result)

try:
    shutil.copy2(source, destination)
    size_mb: float = destination.stat().st_size / (1024 * 1024)
    print(f"Copied {source} to {destination} ({size_mb:.1f} MB).")
finally:
    if connected_here:
        mpr.WNetCancelConnection2W(share, 0, True)
