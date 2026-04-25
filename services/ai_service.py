"""
AI Service - Handles OpenAI integration for STT, LLM, and TTS
"""
import logging
import io
from typing import AsyncGenerator, Optional
from openai import AsyncOpenAI

logger = logging.getLogger(__name__)


class AIService:
    """Service for AI-powered voice processing"""
    
    def __init__(self, api_key: str):
        self.client = AsyncOpenAI(api_key=api_key)
        self.model = "gpt-4o"
        self.voice = "alloy"  # Options: alloy, echo, fable, onyx, nova, shimmer
    
    async def speech_to_text(self, audio_data: bytes, format: str = "wav") -> str:
        """
        Convert speech to text using OpenAI Whisper
        
        Args:
            audio_data: Raw audio bytes
            format: Audio format (wav, mp3, etc.)
            
        Returns:
            Transcribed text
        """
        try:
            audio_file = io.BytesIO(audio_data)
            audio_file.name = f"audio.{format}"
            
            response = await self.client.audio.transcriptions.create(
                model="whisper-1",
                file=audio_file,
                language="hi"  # Support Hindi/English mix
            )
            
            text = response.text
            logger.info(f"STT Result: {text}")
            return text
        except Exception as e:
            logger.error(f"STT Error: {e}")
            return ""
    
    async def chat_completion(
        self, 
        message: str, 
        conversation_history: Optional[list] = None,
        system_prompt: Optional[str] = None
    ) -> str:
        """
        Get AI response using GPT-4
        
        Args:
            message: User message
            conversation_history: Previous messages
            system_prompt: System instructions for AI
            
        Returns:
            AI response text
        """
        try:
            if system_prompt is None:
                system_prompt = """You are a helpful AI voice assistant for Exotel. 
You can communicate in Hindi and English. 
Keep responses concise and natural for voice conversation.
Greet users warmly and help them with their queries."""
            
            messages = [{"role": "system", "content": system_prompt}]
            
            if conversation_history:
                messages.extend(conversation_history)
            
            messages.append({"role": "user", "content": message})
            
            response = await self.client.chat.completions.create(
                model=self.model,
                messages=messages,
                max_tokens=150,
                temperature=0.7
            )
            
            ai_response = response.choices[0].message.content
            logger.info(f"AI Response: {ai_response}")
            return ai_response
        except Exception as e:
            logger.error(f"Chat Error: {e}")
            return "Maaf kijiye, main aapki baat samajh nahi paya. Kripya dobara koshish karein."
    
    async def text_to_speech(self, text: str) -> bytes:
        """
        Convert text to speech using OpenAI TTS
        
        Args:
            text: Text to convert
            
        Returns:
            Audio bytes (MP3 format)
        """
        try:
            response = await self.client.audio.speech.create(
                model="tts-1",
                voice=self.voice,
                input=text,
                response_format="mp3"
            )
            
            audio_data = response.content
            logger.info(f"TTS generated for: {text[:50]}...")
            return audio_data
        except Exception as e:
            logger.error(f"TTS Error: {e}")
            return b""
    
    async def text_to_speech_stream(self, text: str) -> AsyncGenerator[bytes, None]:
        """
        Stream text to speech for real-time playback
        
        Args:
            text: Text to convert
            
        Yields:
            Audio chunks
        """
        try:
            response = await self.client.audio.speech.create(
                model="tts-1",
                voice=self.voice,
                input=text,
                response_format="mp3"
            )
            
            # Stream in chunks
            chunk_size = 1024
            audio_data = response.content
            
            for i in range(0, len(audio_data), chunk_size):
                yield audio_data[i:i + chunk_size]
                
        except Exception as e:
            logger.error(f"TTS Stream Error: {e}")
            yield b""
