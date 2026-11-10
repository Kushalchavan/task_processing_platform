# Decisions

Format: what I chose, what I rejected, why, and what it costs me.

## 1. Redis list as the queue (v1)
- Chose: Redis list (LPUSH / BRPOP).
- Rejected: Kafka (too heavy for this scale), Postgres-only queue (may revisit in v2).
- Why: simplest way to learn queue concepts.
- Cost: BRPOP removes the item immediately. If a worker crashes after popping,
  the task is lost from the queue. Fix in v2.

## 2. Only task IDs go in the queue
- Why: Postgres is the single source of truth. Redis can lose data on restart.

## 3. Save to DB first, then enqueue
- Why: never have a queued ID with no DB row.
- Cost: a crash between the two steps leaves a `pending` task that is never
  queued (dual-write problem). Fix in v2 with a reaper.

## 4. Raw SQL via psycopg, no ORM
- Why: I can see exactly what runs. Learning SQL matters more right now.

## 5. Atomic claim: UPDATE ... WHERE status = 'pending'
- Why: two workers can never run the same task, enforced by the database.