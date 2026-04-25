"""
Sarvam AI Service - Handles TTS and STT for Indian languages

Sarvam AI provides high-quality Text-to-Speech and Speech-to-Text
services optimized for Indian languages like Hindi, Tamil, Telugu, etc.

API Documentation: https://docs.sarvam.ai/
"""
import logging
import io
import base64
from typing import Optional, AsyncGenerator
import aiohttp

logger = logging.getLogger(__name__)


class SarvamService:
    """Service for Sarvam AI TTS and STT"""
    
    BASE_URL = "https://api.sarvam.ai"
    
    # Available voices for TTS (Updated list from Sarvam API)
    VOICES = {
        # Female voices
        "anushka": "Female - natural and friendly",
        "manisha": "Female - professional",
        "vidya": "Female - warm",
        "arya": "Female - clear",
        "ritu": "Female - pleasant",
        "priya": "Female - soft",
        "neha": "Female - energetic",
        "pooja": "Female - calm",
        "simran": "Female - youthful",
        "kavya": "Female - expressive",
        "ishita": "Female - mature",
        "shreya": "Female - sweet",
        "roopa": "Female - gentle",
        "amelia": "Female - international",
        "sophia": "Female - sophisticated",
        "tanya": "Female - modern",
        "shruti": "Female - traditional",
        "suhani": "Female - cheerful",
        "kavitha": "Female - South Indian accent",
        "rupali": "Female - corporate",
        # Male voices
        "abhilash": "Male - professional",
        "karun": "Male - warm",
        "hitesh": "Male - clear",
        "aditya": "Male - friendly",
        "rahul": "Male - natural",
        "rohan": "Male - confident",
        "amit": "Male - mature",
        "dev": "Male - youthful",
        "ratan": "Male - authoritative",
        "varun": "Male - casual",
        "manan": "Male - expressive",
        "sumit": "Male - energetic",
        "kabir": "Male - deep",
        "aayan": "Male - modern",
        "shubh": "Male - pleasant",
        "ashutosh": "Male - traditional",
        "advait": "Male - sophisticated",
        "anand": "Male - friendly",
        "tarun": "Male - professional",
        "sunny": "Male - upbeat",
        "mani": "Male - South Indian accent",
        "gokul": "Male - traditional",
        "vijay": "Male - corporate",
        "mohit": "Male - young",
        "rehan": "Male - expressive",
        "soham": "Male - calm"
    }
    
    # Language codes
    LANGUAGES = {
        "hi-IN": "Hindi",
        "ta-IN": "Tamil",
        "te-IN": "Telugu",
        "kn-IN": "Kannada",
        "ml-IN": "Malayalam",
        "mr-IN": "Marathi",
        "bn-IN": "Bengali",
        "gu-IN": "Gujarati",
        "pa-IN": "Punjabi",
        "en-IN": "English (Indian)"
    }
    
    def __init__(
        self,
        api_key: str,
        tts_language: str = "hi-IN",
        tts_voice: str = "anushka",
        stt_language: str = "hi"
    ):
        self.api_key = api_key
        self.tts_language = tts_language
        self.tts_voice = tts_voice
        self.stt_language = stt_language
        self.headers = {
            "api-subscription-key": api_key,
            "Content-Type": "application/json"
        }
        self._session: Optional[aiohttp.ClientSession] = None
    
    async def _get_session(self) -> aiohttp.ClientSession:
        """Lazy initialization of aiohttp session"""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session
    
    async def close(self):
        """Close the aiohttp session"""
        if self._session is not None and not self._session.closed:
            await self._session.close()
            self._session = None
    
    async def text_to_speech(
        self,
        text: str,
        language: Optional[str] = None,
        voice: Optional[str] = None
    ) -> bytes:
        """
        Convert text to speech using Sarvam AI TTS
        
        Args:
            text: Text to convert (max 500 characters)
            language: Language code (default: hi-IN)
            voice: Voice to use (default: meera)
            
        Returns:
            Audio bytes (WAV format)
        """
        try:
            url = f"{self.BASE_URL}/text-to-speech"
            
            payload = {
                "inputs": [text[:500]],  # Sarvam has 500 char limit
                "target_language_code": language or self.tts_language,
                "speaker": voice or self.tts_voice,
                "pitch": 0,
                "pace": 1.0,
                "loudness": 1.0,
                "speech_sample_rate": 22050,
                "enable_preprocessing": True
            }
            
            logger.info(f"Sarvam TTS Request: {payload}")
            logger.info(f"Sarvam API Key (first 10 chars): {self.api_key[:10]}...")
            
            session = await self._get_session()
            async with session.post(
                url,
                headers=self.headers,
                json=payload
            ) as response:
                logger.info(f"Sarvam TTS Response Status: {response.status}")
                
                if response.status == 200:
                    result = await response.json()
                    audios = result.get("audios", [])
                    logger.info(f"Sarvam TTS Response: {len(audios)} audio(s) received")
                    # Audio is returned as base64 encoded string
                    audio_base64 = audios[0] if audios else ""
                    if audio_base64:
                        audio_bytes = base64.b64decode(audio_base64)
                        logger.info(f"Sarvam TTS generated: {len(audio_bytes)} bytes")
                        return audio_bytes
                    else:
                        logger.error("Sarvam TTS: No audio data in response")
                        return b""
                else:
                    error_text = await response.text()
                    logger.error(f"Sarvam TTS Error: {response.status} - {error_text}")
                    return b""
                        
        except Exception as e:
            logger.error(f"Sarvam TTS Exception: {e}")
            import traceback
            logger.error(f"Sarvam TTS Traceback: {traceback.format_exc()}")
            return b""
    
    async def speech_to_text(
        self,
        audio_data: bytes,
        language: Optional[str] = None
    ) -> str:
        """
        Convert speech to text using Sarvam AI STT
        
        Args:
            audio_data: Raw audio bytes (WAV format, 16kHz)
            language: Language code (default: hi)
            
        Returns:
            Transcribed text
        """
        try:
            url = f"{self.BASE_URL}/speech-to-text"
            
            # Convert audio to base64
            audio_base64 = base64.b64encode(audio_data).decode('utf-8')
            
            payload = {
                "audio": [{"audio_uri": f"data:audio/wav;base64,{audio_base64}"}],
                "language_code": language or self.stt_language
            }
            
            session = await self._get_session()
            async with session.post(
                url,
                headers=self.headers,
                json=payload
            ) as response:
                if response.status == 200:
                    result = await response.json()
                    transcript = result.get("transcript", "")
                    logger.info(f"Sarvam STT Result: {transcript}")
                    return transcript
                else:
                    error_text = await response.text()
                    logger.error(f"Sarvam STT Error: {response.status} - {error_text}")
                    return ""
                        
        except Exception as e:
            logger.error(f"Sarvam STT Error: {e}")
            return ""
    
    async def text_to_speech_stream(
        self,
        text: str,
        language: Optional[str] = None,
        voice: Optional[str] = None
    ) -> AsyncGenerator[bytes, None]:
        """
        Stream text to speech in chunks
        
        Args:
            text: Text to convert
            language: Language code
            voice: Voice to use
            
        Yields:
            Audio chunks
        """
        try:
            # Sarvam doesn't support streaming, so we yield chunks from full audio
            audio_data = await self.text_to_speech(text, language, voice)
            chunk_size = 1024
            
            for i in range(0, len(audio_data), chunk_size):
                yield audio_data[i:i + chunk_size]
                
        except Exception as e:
            logger.error(f"Sarvam TTS Stream Error: {e}")
            yield b""
    
    def get_supported_voices(self) -> dict:
        """Get list of supported voices"""
        return self.VOICES
    
    def get_supported_languages(self) -> dict:
        """Get list of supported languages"""
        return self.LANGUAGES
