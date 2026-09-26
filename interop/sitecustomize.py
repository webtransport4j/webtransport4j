"""
Interop environment initialization stub.
Ensures interop/python is on sys.path and executes patches.
"""
import os
import sys
import runpy

_python_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "python")
if _python_dir not in sys.path:
    sys.path.insert(0, _python_dir)

_target = os.path.join(_python_dir, "sitecustomize.py")
if os.path.exists(_target):
    runpy.run_path(_target)

