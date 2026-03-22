---
description: Mandatory pre-response checklist — run before ANY constructive work
---

# Pre-Response Checklist

1) BEFORE EVERY RESPONSE — NO EXCEPTIONS:
   Read the user's request and ask yourself:
   - Will my response contain any code, commands, file modifications, 
     or terminal instructions?
   - If YES → STOP. Do not write any code. Do not analyze files. 
     Ask the user this exact question first:
     "Do you want me to implement this, or give you steps to do it yourself?"
   - Only proceed after the user explicitly replies.
   - This rule overrides ALL other steps including brainstorm and debugging.

2) Before answering ANY user request that involves building, modifying, debugging, or designing, you MUST complete these steps IN ORDER. Do NOT skip ahead to implementation.

## Step 1: Classify the Request
Determine the type of work:
- **Creative/Constructive** (new feature, new script, architecture change) → Go to Step 2
- **Bug fix / Error** → Read `.agent/skills/debugger/SKILL.md` first, then proceed
- **Kalshi-related** → Read `.agent/skills/kalshi-api-debug/SKILL.md` first, then proceed
- **Simple question / clarification** → Answer directly, no skill needed

## Step 2: Brainstorm FIRST (Mandatory for Creative Work)
// turbo
Read `.agent/skills/brainstorm/SKILL.md` and follow its process COMPLETELY:
1. Understand current context (review files, PRD, prior decisions)
2. Ask questions ONE AT A TIME
3. Clarify non-functional requirements
4. Present Understanding Lock — get explicit user confirmation
5. Explore 2-3 design approaches
6. Present the chosen design incrementally
7. Maintain decision log
8. Do NOT write code until the user confirms the design

## Step 3: Validate Against Project Goals
// turbo
Read `.agent/skills/project-goal-validator/SKILL.md` and confirm:
- Does this align with the PRD phases?
- Does it follow Flat, Free, Native principles?
- Are we in the correct phase for this work?

## Step 4: Check Data Pipeline Patterns
// turbo
If the work involves data ingestion or ETL, read `.agent/skills/data_engineer_pipeline_expert/SKILL.md` for best practices.

## Step 5: Implement
Only after Steps 1-4 are complete, proceed with implementation.

## Git Push Reminder
After any response where 2 or more files were created or modified,
add this at the very end of your response:
"🔀 Git reminder — good time to push your changes."
Then provide these exact commands:
```
git add .
git commit -m "<describe what changed in one line>"
git push origin main
```
Fill in the commit message yourself based on what was actually changed.
Do NOT push anything yourself. Just provide the commands.