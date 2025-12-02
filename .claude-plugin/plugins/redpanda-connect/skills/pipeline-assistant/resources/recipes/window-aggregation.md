# Window-Based Aggregation

**Pattern**: Aggregation - Time Windows
**Difficulty**: Advanced
**Components**: group_by_value, mapping
**Use Case**: Aggregate messages by key within time windows

## Overview

Group and aggregate messages by key (e.g., user_id) to compute statistics like counts and sums. Essential for analytics and reporting pipelines.

## Configuration

See [`window-aggregation.yaml`](./window-aggregation.yaml)

## Key Concepts

### Group By Value
Groups messages with same key value.

### Aggregation Functions
- count: Total messages
- fold: Sum/reduce values
- map_each: Transform arrays

## Related

- [Stateful Counter](stateful-counter.md)
