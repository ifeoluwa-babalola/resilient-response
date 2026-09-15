# Resilient Emergency Response Dispatch Engine

An event-driven emergency dispatch engine built with FastAPI, Redis Streams, PostGIS, and PostgreSQL. Designed for high availability and strict reliability under degraded network conditions and infrastructure failures.

---

## Reliability & Fault Tolerance Matrix

The project focuses on resilience under stress. The core state engine enforces six deterministic failure protections:

| Scenario / Stress Test | Failure Mode | System Safeguard | Guaranteed Outcome |
| :--- | :--- | :--- | :--- |
| **Duplicate Request Storm** | Client sends 5 identical HTTP requests simultaneously | PostgreSQL composite unique key (`organization_id`, `submission_id`) | **Exactly 1 incident created** |
| **Zero Available Responders** | All units offline or at maximum capacity | Automated fallback check in routing engine | **Immediate `ESCALATED_TO_SUPERVISOR` routing** |
| **ACK vs Timeout Race** | Responder ACKs an incident at the exact millisecond of timeout sweep | Database row-level locking (`FOR UPDATE`) | **Single valid state transition** (ACK or Timeout, never corrupt state) |
| **Redis Broker Failure** | Ingestion message queue crashes or drops network connection | Circuit breaker reroutes payload to PostgreSQL `ingestion_events` | **Durable HTTP 202 response; auto-drained on Redis recovery** |
| **Worker Crash Post-DB Write** | Background consumer dies after writing record but before ACKing stream | Worker restarts and re-reads stream message | **`ON CONFLICT DO NOTHING` prevents duplicate processing** |
| **SMS Gateway Outage** | External SMS delivery fails or times out completely | Asynchronous notifications table decoupled from core assignment transaction | **Assignment remains valid; notification retries independently** |

---

## Architecture Overview

```text
[ USSD / API Client ]
         │
         ▼
[ FastAPI Ingestion ]
    │            │
 (Healthy)   (Redis Down)
    │            │
    ▼            ▼
[ Redis ]  [ PostgreSQL: ingestion_events ]
 Stream          │ (Fallback Buffer)
    │            │
    └─────┬──────┘
          ▼
   [ Stream Worker ]
          │
          ▼
 [ Spatial Router (PostGIS) ] ──(No Available Responders)──► [ Supervisor Escalation ]
          │
          ▼
   [ Assignment State ] ◄─── (Timeout Sweep: Escalator Worker)
          │
          ▼
[ Async Notification Queue ] ──► [ Mock SMS Provider ]