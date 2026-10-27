# Working Rules

## Planning

Before making code changes:
- Explain the approach.
- List files to be modified.
- Mention assumptions.

## Implementation

- Prefer minimal changes.
- Avoid changing unrelated files.
- Explain significant code changes.
- Pipelines are Databricks notebooks of plain top-level PySpark cells (no wrapper functions). See README "Writing pipeline code".

## Tracking

Maintain TASK_LOG.md with:
- Task
- Files changed
- Summary
- Completion status

## Verification

Before completion:
- Run tests if available.
- Report results.
- Report any unresolved issues.

## Final Summary

Provide:
- Files modified
- Functions added/updated
- Key decisions made
- Remaining work
