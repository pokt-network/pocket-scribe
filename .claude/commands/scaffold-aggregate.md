---
description: Scaffold a new continuous aggregate. Wraps the add-aggregate skill.
---

Invoke the `add-aggregate` skill.

Expected arguments: `/scaffold-aggregate <name> <bucket_size>`

Example: `/scaffold-aggregate rewards_hourly '1 hour'`

The skill will ask for source tables, dimension columns, aggregations, and consumers_needed.
