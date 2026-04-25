"""
Debug script for outgoing calls
"""
import asyncio
import httpx
import json

WEBHOOK_BASE_URL = "https://intermolecular-useably-forest.ngrok-free.dev"

async def make_outgoing_call():
    """Make a test outgoing call"""
    url = f"{WEBHOOK_BASE_URL}/calls/outgoing"
    
    payload = {
        "to_number": "+919307001740",
        "greeting_message": "Namaste! Main Exotel AI assistant hoon. Aapko call kiya gaya hai."
    }
    
    print(f"📞 Making outgoing call to: {payload['to_number']}")
    print(f"🔗 URL: {url}")
    print(f"📤 Payload: {json.dumps(payload, indent=2)}")
    
    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                url,
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=30
            )
            
            print(f"\n📊 Status: {response.status_code}")
            result = response.json()
            print(f"📦 Response: {json.dumps(result, indent=2)}")
            
            if response.status_code == 200 and result.get("success"):
                print(f"\n✅ Call initiated successfully!")
                print(f"📞 Call SID: {result.get('call_sid')}")
                print(f"💬 Message: {result.get('message')}")
            else:
                print(f"\n❌ Call initiation failed!")
                print(f"Error: {result.get('message')}")
                
    except Exception as e:
        print(f"\n❌ Error: {e}")

if __name__ == "__main__":
    print("🚀 Outgoing Call Debug Script")
    print("=" * 60)
    asyncio.run(make_outgoing_call())
