# Survivor Lab fixture matrix

Each JSON file is a complete `schema_version = 1` input bundle with an `expected` metadata block used only by tests. The metadata is removed before deserialization, so it cannot influence classification.

The matrix covers valid execution, each deterministic ambiguity category, mutation-family hypotheses, selection escape, and insufficient evidence. No fixture references a project import or requires executable source.
