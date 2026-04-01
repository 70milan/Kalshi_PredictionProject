---
description: Mandatory pre-response checklist — run before ANY constructive work
---

# Pre-Response Checklist
# PredictIQ Agent — Behavioral Rules
# Version: 1.1
# Location: .cursor/rules/pre-response-checklist or .cursorrules

---

## RULE 1 — IMPLEMENTATION GATE (NO EXCEPTIONS)

BEFORE EVERY RESPONSE, ask yourself:
- Will my response contain any code, commands, file modifications,
  or terminal instructions?

If YES:
  - STOP.
  - Do not write any code.
  - Do not analyze files.
  - Do not proceed with implementation.
  - Ask the user this exact question first:
    "Do you want me to implement this, or give you steps to do it yourself?"
  - Wait for an explicit reply before proceeding.

If user says YES to implementation:
  - Proceed to Rule 2.

If user says NO to implementation:
  - Provide an ordered list of:
    - Exact terminal/CMD commands (copy-pasteable)
    - File paths and line numbers for any manual edits
    - Code snippets to paste (not full files)
    - DuckDB/SQL queries if data work is involved
  - Keep it tight. Do not over-explain.

For trivial changes (5 lines or fewer, single file):
  - Proceed directly but state in one line what you are about to do
    before doing it.

This rule overrides ALL other steps including brainstorm and debugging.

---

## RULE 2 — CLASSIFY BEFORE ACTING

Determine the type of work before doing anything else:

- Creative/Constructive (new feature, new script, architecture change)
  → Go to Rule 3

- Bug fix / Error
  → Read .agent/skills/debugger/SKILL.md first, then proceed

- Kalshi-related
  → Read .agent/skills/kalshi-api-debug/SKILL.md first, then proceed

- State audit / context sync / project checkpoint
  → Read .agent/skills/state-audit/SKILL.md and follow it completely

- Simple question / clarification
  → Answer directly, no skill needed

---

## RULE 3 — BRAINSTORM FIRST (MANDATORY FOR CREATIVE WORK)

// turbo
Read .agent/skills/brainstorm/SKILL.md and follow its process completely:

1. Understand current context — review files, PRD, prior decisions
2. Ask questions ONE AT A TIME — never batch questions
3. Clarify non-functional requirements
4. Present Understanding Lock — get explicit user confirmation
5. Explore 2-3 design approaches
6. Present the chosen design incrementally
7. Maintain a decision log
8. Do NOT write code until the user confirms the design

---

## RULE 4 — VALIDATE AGAINST PROJECT GOALS

// turbo
Read .agent/skills/project-goal-validator/SKILL.md and confirm:

- Does this align with the PRD phases?
- Does it follow Flat, Free, Native principles?
- Are we in the correct phase for this work?
- Does it respect the $0/month constraint?

If any answer is NO — raise it with the user before proceeding.

---

## RULE 5 — CHECK DATA PIPELINE PATTERNS

// turbo
If the work involves data ingestion, ETL, or schema design:
Read .agent/skills/data_engineer_pipeline_expert/SKILL.md
for best practices before writing any code.

---

## RULE 6 — IMPLEMENT

Only after Rules 1-5 are complete, proceed with implementation.

Codebase standards (strictly enforced):
- No emojis anywhere — not in code, logs, print statements, or comments
- Flat procedural scripts — no deep class hierarchies
- No managed cloud platforms — local native only
- DuckDB for Bronze, PySpark + Delta Lake for Silver/Gold
- Every script must follow the two-file pattern (latest.parquet + TIMESTAMP.parquet)
- Every script must include dedup logic (.last_url or .seen_urls)

---

## RULE 7 — GIT PUSH REMINDER

After any response where 2 or more files were created or modified,
add this block at the very end of your response:

[GIT] Good time to push your changes.

  git add .
  git commit -m "<describe what changed in one line>"
  git push origin main

Fill in the commit message based on what was actually changed.
Do NOT push anything yourself. Just provide the commands.

---

## RULE 8 — STATE AUDIT

When the user says any of the following:
- "run a state audit"
- "sync context"
- "sync context for Claude"
- "where are we right now"
- "what is done and what is pending"
- "give me a project checkpoint"

Read .agent/skills/state-audit/SKILL.md and follow it completely.
Produce the full structured document. Do not summarize or skip sections.