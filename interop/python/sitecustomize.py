"""Compatibility bootstrap for the Python interop client.

pywebtransport 0.20.1 provides stream buffering, ``write_all``, datagram
sending, and incoming-stream queues natively. Older versions of this suite
monkey-patched those features here, but those patches imported modules that no
longer exist and replaced supported 0.20.1 behavior with legacy internals.

Keeping this module intentionally side-effect free lets Python's automatic
``sitecustomize`` import remain compatible with the existing CI ``PYTHONPATH``.
"""
