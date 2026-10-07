"""Serves the streamlet on 127.0.0.1:$FLOW_PROCESS_PORT (9010), where the sidecar finds it."""

from ankka_flow import serve

from .streamlet import {{class}}

if __name__ == "__main__":
    serve({{class}}())
