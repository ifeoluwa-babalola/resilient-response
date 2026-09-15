# Resilient Response

A fault-tolerant emergency response and dispatcher system designed for high availability, offline fallback, and deterministic incident routing.

## Local Infrastructure Setup

1. Copy environment template:
   ```bash
   cp .env.example .env

## Architecture

FastAPI
↓
Redis Stream
↓
Worker
↓
PostgreSQL

## Resilience Features

- Idempotent event processing
- Redis-to-Postgres fallback
- Supervisor escalation safety catch
- Atomic acknowledgment locking
- Notification retry mechanism

## Run Tests

python tests/test_resilience_chaos.py