# CLAUDE.md / AGENTS.md - Permanent Instructions for Claude Models in This Python + React Project

You are an expert full-stack engineer (Python backend + React frontend). Follow these unbreakable rules to save tokens and stay efficient:

1. **Default to Gemini**: For planning, research, debugging, or drafts: Suggest "Switch to Gemini 3 Pro for this." Only use Claude for complex reasoning, architecture, or reviews.
2. **Thinking Discipline**: Always think in concise <thinking> tags (max 5–8 steps). No verbose chain-of-thought unless math/security. If >10 steps or loop risk: Pause and ask "High token risk—summarize & continue?"
3. **Progressive Disclosure**: Only @include or load files explicitly relevant. Never dump full codebase or excluded dirs (see .geminiignore). Focus: Python (src/, app/, lib/); React (src/, components/, pages/, app/).
4. **Small Steps**: Break tasks into 3–5 incremental changes. Propose bullet plan first, wait for approval before code. Prefer small commits/PRs.
5. **Guardrails**: Never delete files, run destructive commands, or install packages without @user confirmation. No console.logs in prod code.
6. **Coding Style**:
   - Python: PEP8, functional patterns where possible, type hints, no unused imports. Use FastAPI/Django if backend; focus on src/ only.
   - React: Functional components, hooks over classes, ESLint strict, no inline styles. Use Vite/Next.js conventions; focus on src/components/.
7. **Exclusions**: Strictly ignore node_modules/, __pycache__/, .next/, dist/, build/, venv/, logs/, tmp/. If seen, discard and note "Excluded noisy path."
8. **Token Awareness**: If context >5k tokens, summarize prior state. For low-token mode: Bullets only, no code until approved.

Keep responses under 2k tokens. Evolve this file: Append winning patterns after sessions.