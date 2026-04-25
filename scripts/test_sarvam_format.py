"""
Dev script — verify Sarvam TTS output format.
Run once before deploying Fix #1 to confirm actual sample rate.

Usage:
    SARVAM_API_KEY=your_key python scripts/test_sarvam_format.py
"""
import asyncio
import base64
import io
import os
import wave

import aiohttp

SARVAM_TTS_URL = "https://api.sarvam.ai/text-to-speech"


async def main():
    api_key = os.environ.get("SARVAM_API_KEY", "")
    if not api_key:
        raise ValueError("Set SARVAM_API_KEY environment variable")

    payload = {
        "inputs": ["Namaste, yeh ek chhota sa test hai."],
        "target_language_code": "hi-IN",
        "speaker": "anushka",
        "pitch": 0,
        "pace": 1.0,
        "loudness": 1.0,
        "speech_sample_rate": 22050,
        "enable_preprocessing": True,
    }

    print("Calling Sarvam TTS API...")
    async with aiohttp.ClientSession() as session:
        async with session.post(
            SARVAM_TTS_URL,
            headers={"api-subscription-key": api_key, "Content-Type": "application/json"},
            json=payload,
        ) as resp:
            if resp.status != 200:
                raise Exception(f"Sarvam error {resp.status}: {await resp.text()}")
            data = await resp.json()

    wav_bytes = base64.b64decode(data["audios"][0])

    with open("sarvam_test.wav", "wb") as f:
        f.write(wav_bytes)
    print(f"Saved: sarvam_test.wav ({len(wav_bytes)} bytes)")

    with wave.open(io.BytesIO(wav_bytes)) as wf:
        print(f"\n--- WAV Format ---")
        print(f"Sample rate  : {wf.getframerate()} Hz")
        print(f"Channels     : {wf.getnchannels()}")
        print(f"Bit depth    : {wf.getsampwidth() * 8}-bit")
        print(f"Duration     : {wf.getnframes() / wf.getframerate():.2f}s")
        print(f"PCM size     : {wf.getnframes() * wf.getnchannels() * wf.getsampwidth()} bytes")

        if wf.getframerate() != 22050:
            print(f"\n⚠️  WARNING: Sarvam returned {wf.getframerate()} Hz, not 22050 Hz!")
            print("   Fix #1 is confirmed necessary.")
        else:
            print("\n✅ Sample rate matches expected 22050 Hz.")


if __name__ == "__main__":
    asyncio.run(main())
