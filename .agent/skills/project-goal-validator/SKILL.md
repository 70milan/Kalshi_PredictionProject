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

## 🚨 Use when:
- Creating new pollers.
- Refactoring existing ingestion logic.
- Planning the transition from Bronze to Silver layers.
