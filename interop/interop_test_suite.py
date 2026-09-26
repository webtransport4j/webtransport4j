#!/usr/bin/env python3
"""
Draft-16 Interoperability Test Suite (Root Forwarder)
Directs execution to interop/python/interop_test_suite.py
"""
import os
import sys
import asyncio

_python_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "python")
if _python_dir not in sys.path:
    sys.path.insert(0, _python_dir)

import interop_test_suite

if __name__ == "__main__":
    asyncio.run(interop_test_suite.main())
