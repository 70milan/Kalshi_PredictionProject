---
name: state-audit
description: "Use this skill whenever the user asks for a project state 
summary, context sync, audit, or checkpoint. Triggers include: 'run a 
state audit', 'sync context', 'where are we', 'what is done', 
'summarize current state', or before starting a new phase. 
Produces a structured markdown document covering directory tree, 
script inventory, Bronze data on disk, Silver status, Docker status, 
and pending work. Output is designed to be fed to an external AI 
reviewer with no filesystem access."
---

# State Audit Skill

## When To Use
Trigger this skill when the user asks any of the following:
- "Run a state audit"
- "Sync context for Claude"
- "Where are we right now"
- "What is done and what is pending"
- "Give me a project checkpoint"
- Before starting any new phase (Bronze complete, Silver starting etc)

## Output Format
Produce a single markdown document with these exact sections 
in this exact order:

### 1. Directory Tree
[full tree command output with file sizes]

### 2. Scripts Inventory
[every .py file — path, purpose, status, last modified, functions]

### 3. Bronze Layer — Actual Data on Disk
[for every bronze/ subfolder — file count, sizes, DuckDB schema query, 
row count, 3 sample rows, MIN/MAX ingested_at]

### 4. Reference Files
[every file in reference/ — row count, columns, 3 sample rows]

### 5. Silver Layer Status
[what exists, what scripts exist, what is pending]

### 6. Docker Status
[Dockerfile contents, docker-compose.yml contents, docker ps output]

### 7. Agent Skills and Workflow Files
[list all .agent/ files, full contents of pre-response-checklist.md]

### 8. Pending Work Assessment
[done and verified / partially done or broken / not started]

### 9. Open Questions and Blockers
[any known issues, errors, blockers]

## Rules
- Use actual file contents and real DuckDB query results
- Never assume or approximate — read from disk
- Do not skip any section
- Do not summarize schema — show actual column names and types
- This document will be read by an AI with no filesystem access 
  so precision is non-negotiable