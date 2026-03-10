---
name: python-react-exclude-junk
description: Filters noise for Python/React tasks
triggers: list_dir, search, @workspace, read_file, analyze, refactor, python, react
---

Before file ops/context:
- Exclude: node_modules, __pycache__, .next, dist, build, venv, coverage, logs, tmp.
- If detected: "Excluded [N] noisy files from [dir]" and proceed clean.
- Python: src/app/lib only unless deps requested.
- React: src/components/pages only; ignore node_modules unless "debug import".