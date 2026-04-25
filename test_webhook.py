"""
Webhook Testing Script for Exotel AI Voice System

This script helps you test your webhook endpoints before going live.

Usage:
    python test_webhook.py

Make sure your server is running first:
    python start_server.py
"""
import asyncio
import json
import sys
from urllib.parse import urljoin

try:
    import httpx
except ImportError:
    print("❌ httpx not installed. Installing...")
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "httpx"])
    import httpx

# Your webhook base URL
WEBHOOK_BASE_URL = "https://intermolecular-useably-forest.ngrok-free.dev"


async def test_health_endpoint():
    """Test if server is running"""
    print("\n" + "="*60)
    print("TEST 1: Health Check")
    print("="*60)
    
    url = urljoin(WEBHOOK_BASE_URL, "/health")
    print(f"🔗 URL: {url}")
    
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(url, timeout=10)
            print(f"📊 Status: {response.status_code}")
            print(f"📦 Response: {response.json()}")
            
            if response.status_code == 200:
                print("✅ Health check PASSED")
                return True
            else:
                print("❌ Health check FAILED")
                return False
    except Exception as e:
        print(f"❌ Error: {e}")
        print("💡 Make sure your server is running: python start_server.py")
        return False


async def test_config_endpoint():
    """Test configuration endpoint"""
    print("\n" + "="*60)
    print("TEST 2: Configuration Check")
    print("="*60)
    
    url = urljoin(WEBHOOK_BASE_URL, "/config")
    print(f"🔗 URL: {url}")
    
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(url, timeout=10)
            print(f"📊 Status: {response.status_code}")
            config = response.json()
            print(f"📦 Response: {json.dumps(config, indent=2)}")
            
            if response.status_code == 200:
                print("✅ Config check PASSED")
                return True
            else:
                print("❌ Config check FAILED")
                return False
    except Exception as e:
        print(f"❌ Error: {e}")
        return False


async def test_voicebot_webhook():
    """Test Voicebot webhook with simulated Exotel request"""
    print("\n" + "="*60)
    print("TEST 3: Voicebot Webhook (Simulated Exotel)")
    print("="*60)
    
    url = urljoin(WEBHOOK_BASE_URL, "/webhook/voicebot")
    print(f"🔗 URL: {url}")
    
    # Simulate initial call (no user input)
    payload = {
        "call_sid": "TEST123456",
        "from": "+919876543210",
        "to": "+917941056502",
        "user_input": "",  # Empty for greeting
        "conversation_id": "TEST123456"
    }
    
    print(f"📤 Request: {json.dumps(payload, indent=2)}")
    
    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                url, 
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=30
            )
            print(f"📊 Status: {response.status_code}")
            result = response.json()
            print(f"📦 Response: {json.dumps(result, indent=2)}")
            
            if response.status_code == 200 and "text" in result:
                print("✅ Voicebot webhook PASSED (Standard JSON)")
                return True
            else:
                print("❌ Voicebot webhook FAILED")
                return False
    except Exception as e:
        print(f"❌ Error: {e}")
        return False


async def test_voicebot_with_input():
    """Test Voicebot with user input"""
    print("\n" + "="*60)
    print("TEST 4: Voicebot with User Input")
    print("="*60)
    
    url = urljoin(WEBHOOK_BASE_URL, "/webhook/voicebot")
    print(f"🔗 URL: {url}")
    
    # Simulate user speaking
    payload = {
        "call_sid": "TEST123456",
        "from": "+919876543210",
        "to": "+917941056502",
        "user_input": "Namaste, aap kaise hain?",
        "conversation_id": "TEST123456"
    }
    
    print(f"📤 Request: {json.dumps(payload, indent=2)}")
    
    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                url, 
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=30
            )
            print(f"📊 Status: {response.status_code}")
            result = response.json()
            print(f"📦 Response: {json.dumps(result, indent=2)}")
            
            if response.status_code == 200 and "text" in result:
                print("✅ Voicebot with input PASSED")
                print(f"🤖 AI Response: {result['text']}")
                # Note: audio field removed for Exotel compatibility
                return True
            else:
                print("❌ Voicebot with input FAILED")
                return False
    except Exception as e:
        print(f"❌ Error: {e}")
        return False


async def test_voicebot_get_request():
    """Test Voicebot webhook with GET request"""
    print("\n" + "="*60)
    print("TEST 4.5: Voicebot Webhook (GET Request)")
    print("="*60)
    
    url = urljoin(WEBHOOK_BASE_URL, "/webhook/voicebot")
    params = {
        "call_sid": "GET_TEST_123",
        "from": "+919307001740",
        "to": "+917941056502",
        "user_input": "Hello from GET"
    }
    print(f"🔗 URL: {url}")
    print(f"📤 Params: {params}")
    
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(url, params=params, timeout=10)
            print(f"📊 Status: {response.status_code}")
            result = response.json()
            print(f"📦 Response: {json.dumps(result, indent=2)}")
            
            if response.status_code == 200 and "text" in result:
                print("✅ Voicebot GET request PASSED")
                return True
            else:
                print("❌ Voicebot GET request FAILED")
                return False
    except Exception as e:
        print(f"❌ Error: {e}")
        return False


async def test_sarvam_tts():
    """Test Sarvam TTS endpoint"""
    print("\n" + "="*60)
    print("TEST 5: Sarvam TTS")
    print("="*60)
    
    url = urljoin(WEBHOOK_BASE_URL, "/webhook/sarvam/tts")
    print(f"🔗 URL: {url}")
    
    payload = {
        "text": "Namaste! Main Exotel AI assistant hoon.",
        "language": "hi-IN",
        "voice": "anushka"
    }
    
    print(f"📤 Request: {json.dumps(payload, indent=2)}")
    
    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                url,
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=30
            )
            print(f"📊 Status: {response.status_code}")
            result = response.json()
            
            if response.status_code == 200 and result.get("success"):
                audio_size = len(result.get("audio", ""))
                print(f"✅ Sarvam TTS PASSED")
                print(f"🎙️  Audio generated: {audio_size} bytes (base64)")
                print(f"🗣️  Voice: {result.get('voice')}")
                return True
            else:
                print(f"❌ Sarvam TTS FAILED: {result.get('error')}")
                return False
    except Exception as e:
        print(f"❌ Error: {e}")
        return False


async def run_all_tests():
    """Run all tests"""
    print("\n" + "🚀"*30)
    print("EXOTEL WEBHOOK TESTING SUITE")
    print("🚀"*30)
    print(f"\nTesting URL: {WEBHOOK_BASE_URL}")
    
    results = []
    
    # Run tests
    results.append(("Health Check", await test_health_endpoint()))
    results.append(("Config Check", await test_config_endpoint()))
    results.append(("Voicebot Webhook (POST)", await test_voicebot_webhook()))
    results.append(("Voicebot with Input (POST)", await test_voicebot_with_input()))
    results.append(("Voicebot GET Request", await test_voicebot_get_request()))
    results.append(("Sarvam TTS", await test_sarvam_tts()))
    
    # Summary
    print("\n" + "="*60)
    print("TEST SUMMARY")
    print("="*60)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for name, result in results:
        status = "✅ PASSED" if result else "❌ FAILED"
        print(f"{status}: {name}")
    
    print(f"\n📊 Total: {passed}/{total} tests passed")
    
    if passed == total:
        print("\n🎉 All tests passed! Your webhook is ready for Exotel!")
    else:
        print("\n⚠️  Some tests failed. Check the errors above.")
    
    return passed == total


if __name__ == "__main__":
    print("Make sure your server is running: python start_server.py")
    print("Press Ctrl+C to cancel\n")
    
    try:
        success = asyncio.run(run_all_tests())
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n\n👋 Testing cancelled")
        sys.exit(0)
