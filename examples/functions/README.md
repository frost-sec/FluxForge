# Native function fixtures

echo, add and fan_in demonstrate successful JSON handlers; spin, crash and log_flood exercise execution containment; large_output exercises Claim-Check. Build them in the C++ Docker toolchain with the Runner CMake project. The fixture output directory is build/examples. Fault functions are used only by isolated tests.

echo returns its input; add expects numeric a/b and returns value=a+b; fan_in sums each object field's value. large_output accepts bytes (default 128KiB) and returns a data string. All use the versioned SDK and runtime compatibility ID configured by the deployment.
