import asyncio
import os
import asyncpg

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres_password@localhost:5432/emergency_db")

async def route_incident(conn: asyncpg.Connection, incident_id: str, lng: float, lat: float, radius_meters: float = 10000.0):
    """
    Finds the best eligible responder using 5 deterministic checks and assigns the incident.
    """
    # 5-Step Routing Query
    find_responder_query = """
        SELECT 
            responder_id,
            name,
            current_active_incidents,
            ST_Distance(location, ST_SetSRID(ST_MakePoint($1, $2), 4326)) AS distance_meters
        FROM responders
        WHERE is_eligible = TRUE                                           -- 1. Eligible?
          AND is_online = TRUE                                             -- 2. Online?
          AND ST_DWithin(location, ST_SetSRID(ST_MakePoint($1, $2), 4326), $3) -- 3. Within Radius?
          AND current_active_incidents < active_capacity                  -- 4. Capacity Available?
        ORDER BY 
            current_active_incidents ASC,                                  -- 5. Lowest Workload First
            distance_meters ASC                                            -- Tie-breaker: Closest distance
        LIMIT 1
        FOR UPDATE; -- Lock row to prevent race conditions
    """

    async with conn.transaction():
        best_responder = await conn.fetchrow(find_responder_query, lng, lat, radius_meters)

        if not best_responder:
            print(f"⚠️ No available responder found for incident {incident_id}")
            return None

        responder_id = best_responder["responder_id"]

        # 1. Create assignment record
        await conn.execute(
            """
            INSERT INTO assignments (incident_id, responder_id, status)
            VALUES ($1, $2, 'ASSIGNED')
            """,
            incident_id, responder_id
        )

        # 2. Increment responder workload
        await conn.execute(
            """
            UPDATE responders 
            SET current_active_incidents = current_active_incidents + 1 
            WHERE responder_id = $1
            """,
            responder_id
        )

        # 3. Update incident status
        await conn.execute(
            """
            UPDATE incidents 
            SET status = 'ASSIGNED' 
            WHERE incident_id = $1
            """,
            incident_id
        )

        print(f"✅ Incident {incident_id} assigned to {best_responder['name']} ({responder_id})")
        return responder_id

async def main():
    pool = await asyncpg.create_pool(dsn=DATABASE_URL)
    async with pool.acquire() as conn:
        # Simulate routing an incident near Central Market (-0.12, 51.50)
        await route_incident(conn, "IR-SUB-001", lng=-0.12, lat=51.50)
    await pool.close()

if __name__ == "__main__":
    asyncio.run(main())