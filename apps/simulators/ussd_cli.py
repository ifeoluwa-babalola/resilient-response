import asyncio
import uuid
import sys
import httpx

API_BASE_URL = "http://localhost:8000"


class USSDSimulator:
    def __init__(self):
        self.session_active = True
        # Track submissions created during this CLI session
        self.history: dict[str, str] = {}  # choice_num -> submission_id

    async def send_incident_payload(self, category: str, location: str, description: str):
        submission_id = f"SUB-{uuid.uuid4().hex[:8].upper()}"
        payload = {
            "submission_id": submission_id,
            "channel": "USSD",
            "category": category,
            "urgency": "HIGH",
            "location_text": location,
            "description": description
        }

        print(f"\n[NETWORK] Dialing USSD Gateway -> Posting to {API_BASE_URL}/v1/incidents...")
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                resp = await client.post(f"{API_BASE_URL}/v1/incidents", json=payload)
                if resp.status_code == 202:
                    data = resp.json()
                    print("\n==========================================")
                    print("📱 USSD RESPONSE (Screen):")
                    print(f"Emergency report accepted!\nTracking ID: {data['submission_id']}\nIncident ID: {data['incident_id']}")
                    print("==========================================")
                    self.history[str(len(self.history) + 1)] = submission_id
                else:
                    print(f"\n❌ USSD Error ({resp.status_code}): {resp.text}")
        except httpx.RequestError as exc:
            print(f"\n🚨 NETWORK ERROR: Unable to reach gateway API ({exc})")

    async def check_incident_status(self, submission_id: str):
        print(f"\n[NETWORK] Querying status for {submission_id}...")
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                resp = await client.get(f"{API_BASE_URL}/v1/incidents/{submission_id}")
                if resp.status_code == 200:
                    data = resp.json()
                    print("\n==========================================")
                    print("📱 USSD RESPONSE (Screen):")
                    print(f"Incident: {data['incident_id']}")
                    print(f"Status: {data['status']}")
                    print(f"Location: {data['location_text']}")
                    print("==========================================")
                elif resp.status_code == 404:
                    print(f"\n📱 USSD RESPONSE: Report {submission_id} not found.")
                else:
                    print(f"\n❌ USSD Error ({resp.status_code}): {resp.text}")
        except httpx.RequestError as exc:
            print(f"\n🚨 NETWORK ERROR: Unable to reach API ({exc})")

    async def menu_report(self):
        print("\n--- [USSD *123# -> Report Incident] ---")
        print("Select Category:")
        print("1. MEDICAL")
        print("2. FIRE")
        print("3. ACCIDENT")
        print("4. CRIME")
        
        cat_map = {"1": "MEDICAL", "2": "FIRE", "3": "ACCIDENT", "4": "CRIME"}
        cat_choice = input("Option (1-4): ").strip()
        category = cat_map.get(cat_choice, "OTHER")

        location = input("Enter Location (e.g. Central Market): ").strip()
        if not location:
            location = "Unknown Location"

        description = input("Enter Brief Description: ").strip()
        await self.send_incident_payload(category, location, description)

    async def menu_status(self):
        print("\n--- [USSD *123# -> Check Status] ---")
        if self.history:
            print("Recent submissions in session:")
            for k, sub_id in self.history.items():
                print(f" {k}. {sub_id}")
            choice = input("Select number or type Submission ID directly: ").strip()
            target_id = self.history.get(choice, choice)
        else:
            target_id = input("Enter Submission ID (e.g., SUB-XXXX): ").strip()

        if target_id:
            await self.check_incident_status(target_id)

    async def run(self):
        print("\n==========================================")
        print("📞 USSD TELECOM SIMULATOR (*123#)")
        print("==========================================")

        while self.session_active:
            print("\n*123# Main Menu")
            print("1. Report Incident")
            print("2. Check Report Status")
            print("3. Exit")

            choice = input("Select option (1-3): ").strip()
            if choice == "1":
                await self.menu_report()
            elif choice == "2":
                await self.menu_status()
            elif choice == "3":
                print("\nUSSD Session Closed. Goodbye.")
                self.session_active = False
            else:
                print("\nInvalid choice. Try again.")


if __name__ == "__main__":
    sim = USSDSimulator()
    try:
        asyncio.run(sim.run())
    except KeyboardInterrupt:
        print("\nUSSD Session terminated.")
        sys.exit(0)