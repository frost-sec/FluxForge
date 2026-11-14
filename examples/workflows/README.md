# Example workflows

fan-in.json is protobuf JSON for WorkflowSpec. Dependency edges are inferred from stepOutput references. Submit {"a":2,"b":3}; A echoes, B and C each compute 5, D returns {"value":10}. Use the Docker ffctl demo command to publish and verify the full path.
