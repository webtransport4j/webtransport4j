"""Compatibility bootstrap for the Python interop client.

pywebtransport 0.22.0 compatibility shim:
- Tolerates deprecated/removed `log_level` parameter in `ClientConfig`.
- Provides backward-compatible `accept_bidirectional_stream` and
  `accept_unidirectional_stream` methods on `WebTransportSession`.
"""
try:
    import pywebtransport.config
    import pywebtransport.session
    from pywebtransport.exceptions import ConnectionError, SessionError

    # Patch ClientConfig to ignore deprecated log_level kwarg
    _orig_client_config_init = pywebtransport.config.ClientConfig.__init__

    def _patched_client_config_init(self, *args, **kwargs):
        kwargs.pop("log_level", None)
        _orig_client_config_init(self, *args, **kwargs)

    pywebtransport.config.ClientConfig.__init__ = _patched_client_config_init

    async def _pop_stream(queue, is_persistent_none: bool):
        stream = await queue.get()
        if stream is None:
            if is_persistent_none:
                queue.put_nowait(None)
            raise ConnectionError("Session closed")
        return stream

    # Provide accept_bidirectional_stream / accept_unidirectional_stream on WebTransportSession
    if not hasattr(pywebtransport.session.WebTransportSession, "accept_bidirectional_stream"):
        async def _accept_bidirectional_stream(self):
            if hasattr(self, "_incoming_streams_bidi"):
                return await _pop_stream(self._incoming_streams_bidi, True)
            if hasattr(self, "_incoming_bidi_streams"):
                return await _pop_stream(self._incoming_bidi_streams, False)
            raise AttributeError("Session has no incoming bidirectional stream queue")

        pywebtransport.session.WebTransportSession.accept_bidirectional_stream = _accept_bidirectional_stream

    if not hasattr(pywebtransport.session.WebTransportSession, "accept_unidirectional_stream"):
        async def _accept_unidirectional_stream(self):
            if hasattr(self, "_incoming_streams_uni"):
                return await _pop_stream(self._incoming_streams_uni, True)
            if hasattr(self, "_incoming_uni_streams"):
                return await _pop_stream(self._incoming_uni_streams, False)
            raise AttributeError("Session has no incoming unidirectional stream queue")

        pywebtransport.session.WebTransportSession.accept_unidirectional_stream = _accept_unidirectional_stream

except ImportError:
    pass
