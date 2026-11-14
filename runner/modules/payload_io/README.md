# payload_io

Sources: include/fluxforge/object_io.hpp (ObjectIO); include/fluxforge/input_plan.hpp.

## Purpose and implementation

Direct MinIO transfer from short-lived grants, checksum validation, InputPlan JSON Pointer resolution and result uploads.

## Interfaces, dependencies and ownership

Large output enters the persisted COMPLETING phase before upload. Functions never receive S3 credentials; committed URLs remain version-pinned by control.

## Configuration

FF_MAX_PAYLOAD_BYTES defaults 16MiB; inline result threshold 64KiB; total distinct bound-source bytes and assembled input are bounded. At most 256 field bindings.

## Docker validation

Use the root Docker tools wrapper or run the C++ tools image: cmake -S /workspace/runner -B /workspace/runner/build-agent -DFF_BUILD_AGENT=ON; cmake --build /workspace/runner/build-agent -j 4; ctest --test-dir /workspace/runner/build-agent --output-on-failure. No host compiler is needed. Full distributed behavior is verified by the root integration tests, not the isolated CTest alone.
