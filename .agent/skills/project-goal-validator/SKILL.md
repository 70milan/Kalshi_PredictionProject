---
name: project-goal-validator
description: Ensures all code developed adheres to the PredictIQ project goals and follows established architectural principles (Flat, Free, Native). Enforces professional, emoji-free communication and code standards.
---

# PredictIQ Project Goal Validator

Use this skill to validate every code change against the "North Star" goals of the PredictIQ project.

## 🏁 The Project Goal (North Star)
Build a **sophisticated political and economic prediction tool** that uses high-signal data (Kalshi, NewsAPI, GDELT) to provide actionable intelligence, while remaining **completely free to operate** and **native to Windows**.

## 🏗️ Architectural Principles (The "How")
Every script must follow these "Mfer" guidelines:
1. **Flat Code**: Prefer top-level, procedural execution. Avoid deep class hierarchies. Keep it readable for local debugging.
2. **Native Storage**: Use **DuckDB** and **Parquet**. No heavy database servers.
3. **Free Tier First**: 
    - Use smart filtering to stay under rate limits (e.g., fetch Series then Markets).
    - Limit NewsAPI to low-frequency polling.
4. **Project Root Structure**: Always write data to the central `/data` directory at the project root.

## ✅ Validation Checklist
Before finalizing any code, verify:
- [ ] **No Emojis**: Are there zero emojis in the code, comments, or documentation?
- [ ] **Smart Filtering**: Does it avoid the "Haystack" (e.g., thousands of NBA/Weather markets)?
- [ ] **Data Flattening**: Are nested dicts serialized to strings for Parquet compatibility?
- [ ] **Path Handling**: Does it use `os.path.join` and absolute paths where necessary?
- [ ] **Rate Limiting**: Does it include `time.sleep()` in loops?
- [ ] **Testing**: Has the user verified the output in DBeaver or via a `preview()` function?

## 🖋️ Style & Communication
- Use strictly professional, technical language.
- Emojis are prohibited in all internal and external communication.
- Focus on technical rationale and actionable engineering outcomes.

## 🧪 Testing Requirements
1. **Mock or Sample Run**: Always perform a small fetch first to verify schema.
2. **Schema Integrity**: Check that the columns expected by the "Silver Layer" (e.g., `ticker`, `yes_bid_dollars`, `ingested_at`) are present.
3. **Zero-Leaking**: Ensure API keys are loaded via `.env` only.

## No Hardcoded Values
- NEVER hardcode file paths, usernames, IP addresses, or machine-specific values in any script.
- All environment-dependent values MUST be read dynamically (e.g., `os.environ.get()`, `os.path.dirname(__file__)`, `.env` files).
- Use `ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))` for project-relative paths.

## Security & Privacy
- **Zero personal information**: No references to "Milan Barot" or any personal identifiers in committed code.
- **Zero API keys**: All secrets MUST be loaded via `.env` and NEVER appear in source code.
- **Zero test artifacts**: Test result files, debug scripts, scratch outputs, and local-only data MUST NOT be committed.
- **`.gitignore` is mandatory**: Before ANY git operation, verify `.gitignore` covers:
  - `data/` (all Bronze/Silver/Gold data)
  - `.env` and any secret files
  - `__pycache__/`, `.venv/`, `*.pyc`
  - Any test output, debug logs, or scratch files
  - IDE-specific folders (`.idea/`, `.vscode/`)

## Git Workflow Protocol
The following rules apply to ALL git operations for the entire project lifecycle:
1. **Git push is ONLY executed on the user's explicit command.** Never auto-push.
2. **Before ANY commit**, scan all staged files and verify:
   - No hardcoded paths, usernames, or personal information
   - No API keys, tokens, or secrets
   - No test result files or debug scripts
   - `.gitignore` is up to date for any new file types
3. **Step-by-step confirmation required:**
   - Step A: Propose which files to `git add` — wait for user approval
   - Step B: Propose commit message — wait for user approval
   - Step C: Execute `git push` — ONLY after user explicitly says to push
4. Each step requires the user's response before proceeding to the next.

## Use when:
- Creating new pollers.
- Refactoring existing ingestion logic.
- Planning the transition from Bronze to Silver layers.
- Performing ANY git operations (commit, push, branch).
