# parrot.events.v1 proto (FEAT-312 gRPC ingress)

Mudado desde
`packages/ai-parrot/src/parrot/core/events/bus/ingress/proto/README.md`
(ai-parrot@686aba1fe, FEAT-310).

`events.proto` defines the `parrot.events.v1.EventBusIngress` service used
by `GrpcIngress` (`navigator_eventbus/ingress/grpc.py`). The wire-level
proto package name (`parrot.events.v1`) is preserved verbatim — it is a
protocol contract, not a Python import path (see the spec's Codebase
Contract).

The generated modules (`events_pb2.py`, `events_pb2_grpc.py`) are committed
alongside the source `.proto`. They are **generated code** — do not edit
them by hand and keep them excluded from linting.

**FEAT-312**: this directory did not have an `__init__.py` in the ai-parrot
origin (namespace package); one is added here (spec acceptance criterion).

## Regenerating

Requires the optional extra (`pip install navigator-eventbus[grpc]`, which
brings `grpcio-tools`). From the repository root:

```bash
source .venv/bin/activate
python -m grpc_tools.protoc -I src \
  --python_out=src \
  --grpc_python_out=src \
  src/navigator_eventbus/ingress/proto/events.proto
```

Regenerate whenever `events.proto` changes and commit the updated pb2
modules in the same commit. Keep `grpcio` / `grpcio-tools` versions in sync
(same minor) to avoid runtime descriptor incompatibilities.
