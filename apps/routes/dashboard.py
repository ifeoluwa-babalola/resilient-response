import os
import asyncpg
from fastapi import APIRouter, HTTPException, status
from fastapi.responses import HTMLResponse

router = APIRouter(prefix="/dashboard", tags=["Dashboard"])

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:postgres_password@localhost:5432/emergency_db",
)


@router.get("", response_class=HTMLResponse)
async def render_dashboard():
    """Serves the minimal single-page HTML dashboard."""
    html_content = """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <title>Resilient Response Dashboard</title>
        <style>
            body { font-family: monospace; background: #121212; color: #e0e0e0; margin: 20px; }
            h1, h2 { color: #00ffcc; }
            table { width: 100%; border-collapse: collapse; margin-bottom: 20px; }
            th, td { border: 1px solid #333; padding: 10px; text-align: left; }
            th { background-color: #1e1e1e; }
            tr:hover { background-color: #2a2a2a; cursor: pointer; }
            .container { display: flex; gap: 20px; }
            .left { flex: 6; }
            .right { flex: 4; border: 1px solid #333; padding: 15px; background: #181818; }
            button { background: #00ffcc; color: #121212; border: none; padding: 8px 14px; font-weight: bold; cursor: pointer; margin-right: 5px; }
            button:hover { background: #00cca3; }
            .badge { padding: 3px 6px; border-radius: 3px; font-size: 0.85em; }
            .CRITICAL { background: #ff4d4d; color: white; }
            .HIGH { background: #ff9900; color: black; }
            .MEDIUM { background: #ffff00; color: black; }
            .timeline-item { border-left: 2px solid #00ffcc; padding-left: 10px; margin-bottom: 8px; }
        </style>
    </head>
    <body>
        <h1>Resilient Emergency Incident Dashboard</h1>
        <div class="container">
            <div class="left">
                <h2>Active Incidents</h2>
                <table>
                    <thead>
                        <tr>
                            <th>ID</th>
                            <th>Urgency</th>
                            <th>Status</th>
                            <th>Assigned To</th>
                        </tr>
                    </thead>
                    <tbody id="incidents-list"></tbody>
                </table>
            </div>
            
            <div class="right" id="detail-panel">
                <h2>Incident Detail</h2>
                <p>Select an incident from the table to inspect details.</p>
            </div>
        </div>

        <script>
            async function fetchIncidents() {
                const res = await fetch('/dashboard/api/incidents');
                const incidents = await res.json();
                const tbody = document.getElementById('incidents-list');
                tbody.innerHTML = '';

                incidents.forEach(inc => {
                    const tr = document.createElement('tr');
                    tr.onclick = () => loadDetail(inc.incident_id);
                    tr.innerHTML = `
                        <td><strong>${inc.incident_id}</strong></td>
                        <td><span class="badge ${inc.urgency}">${inc.urgency}</span></td>
                        <td>${inc.status}</td>
                        <td>${inc.assigned_to || '—'}</td>
                    `;
                    tbody.appendChild(tr);
                });
            }

            async function loadDetail(id) {
                const res = await fetch(`/dashboard/api/incidents/${id}`);
                const data = await res.json();
                const panel = document.getElementById('detail-panel');

                let timelineHtml = data.timeline.map(t => `
                    <div class="timeline-item">
                        <small>${new Date(t.created_at).toLocaleTimeString()}</small><br>
                        <strong>${t.status}</strong> (${t.actor})
                    </div>
                `).join('');

                panel.innerHTML = `
                    <h2>${data.incident_id}</h2>
                    <p><strong>Urgency:</strong> <span class="badge ${data.urgency}">${data.urgency}</span></p>
                    <p><strong>Location:</strong> ${data.location_text}</p>
                    <p><strong>Description:</strong> ${data.description || 'N/A'}</p>
                    <hr>
                    <h3>Timeline</h3>
                    <div>${timelineHtml || '<p>No timeline events.</p>'}</div>
                    <hr>
                    <div style="margin-top: 15px;">
                        <button onclick="performAction('${id}', 'ack')">ACKNOWLEDGE</button>
                        <button onclick="performAction('${id}', 'resolve')">RESOLVE</button>
                    </div>
                `;
            }

            async function performAction(id, action) {
                await fetch(`/dashboard/api/incidents/${id}/${action}`, { method: 'POST' });
                await fetchIncidents();
                await loadDetail(id);
            }

            fetchIncidents();
            setInterval(fetchIncidents, 3000); // Auto-refresh table every 3s
        </script>
    </body>
    </html>
    """
    return HTMLResponse(content=html_content)


@router.get("/api/incidents")
async def list_dashboard_incidents():
    """Returns active incidents with their currently assigned responder/supervisor."""
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        rows = await conn.fetch(
            """
            SELECT 
                i.incident_id, 
                i.urgency, 
                i.status, 
                COALESCE(r.name, CASE WHEN i.status = 'ESCALATED_TO_SUPERVISOR' THEN 'Supervisor' ELSE NULL END) as assigned_to
            FROM incidents i
            LEFT JOIN assignments a ON i.incident_id = a.incident_id AND a.status = 'ASSIGNED'
            LEFT JOIN responders r ON a.responder_id = r.responder_id
            ORDER BY i.created_at DESC;
            """
        )
        return [dict(r) for r in rows]
    finally:
        await conn.close()


@router.get("/api/incidents/{incident_id}")
async def get_dashboard_incident_detail(incident_id: str):
    """Returns incident detail and recorded state timeline."""
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        inc = await conn.fetchrow(
            "SELECT * FROM incidents WHERE incident_id = $1;", incident_id
        )
        if not inc:
            raise HTTPException(status_code=404, detail="Incident not found")

        timeline = await conn.fetch(
            "SELECT status, actor, created_at FROM incident_timeline WHERE incident_id = $1 ORDER BY created_at ASC;",
            incident_id,
        )

        result = dict(inc)
        result["timeline"] = [dict(t) for t in timeline]
        return result
    finally:
        await conn.close()


@router.post("/api/incidents/{incident_id}/ack")
async def action_acknowledge(incident_id: str):
    """Manual ACK action button."""
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        await conn.execute(
            "UPDATE incidents SET status = 'IN_PROGRESS', ack_deadline = NULL WHERE incident_id = $1;",
            incident_id,
        )
        await conn.execute(
            "UPDATE assignments SET status = 'ACKNOWLEDGED' WHERE incident_id = $1 AND status = 'ASSIGNED';",
            incident_id,
        )
        await conn.execute(
            "INSERT INTO incident_timeline (incident_id, status, actor) VALUES ($1, 'ACKNOWLEDGED', 'DASHBOARD_OPERATOR');",
            incident_id,
        )
        return {"status": "SUCCESS"}
    finally:
        await conn.close()


@router.post("/api/incidents/{incident_id}/resolve")
async def action_resolve(incident_id: str):
    """Manual RESOLVE action button."""
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        await conn.execute(
            "UPDATE incidents SET status = 'RESOLVED', ack_deadline = NULL WHERE incident_id = $1;",
            incident_id,
        )
        await conn.execute(
            "UPDATE assignments SET status = 'COMPLETED' WHERE incident_id = $1;",
            incident_id,
        )
        await conn.execute(
            "INSERT INTO incident_timeline (incident_id, status, actor) VALUES ($1, 'RESOLVED', 'DASHBOARD_OPERATOR');",
            incident_id,
        )
        return {"status": "SUCCESS"}
    finally:
        await conn.close()