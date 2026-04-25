"""
Sarvam AI STT Plugin for LiveKit Agent

Wraps the Sarvam STT API as a LiveKit STT plugin.
Uses multipart file upload (not JSON) as required by the Sarvam API.
"""
import io
import logging
import uuid
import wave

import aiohttp
from livekit import rtc
from livekit.agents import stt, utils

logger = logging.getLogger(__name__)

SARVAM_STT_URL = "https://api.sarvam.ai/speech-to-text"


class STT(stt.STT):
    """Sarvam AI Speech-to-Text for Indian languages"""

    def __init__(
        self,
        *,
        api_key: str,
        language: str = "hi-IN",
    ):
        super().__init__(
            capabilities=stt.STTCapabilities(
                streaming=False,
                interim_results=False,
            ),
        )
        self._api_key = api_key
        self._language = language
        self._session: aiohttp.ClientSession | None = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def _recognize_impl(
        self,
        buffer: utils.AudioBuffer,
        *,
        language,
        conn_options,
    ) -> stt.SpeechEvent:
        # Merge all frames into a single AudioFrame
        frame = utils.merge_frames(buffer)

        # Convert to WAV bytes for Sarvam API
        wav_bytes = self._frame_to_wav(frame)

        logger.info(
            f"Sarvam STT: sending {len(wav_bytes)} bytes WAV "
            f"({frame.sample_rate}Hz, {frame.samples_per_channel} samples)"
        )

        # Sarvam STT requires multipart file upload
        form = aiohttp.FormData()
        form.add_field(
            "file",
            wav_bytes,
            filename="audio.wav",
            content_type="audio/wav",
        )
        form.add_field("language_code", self._language)

        headers = {
            "api-subscription-key": self._api_key,
        }

        session = await self._get_session()
        async with session.post(SARVAM_STT_URL, headers=headers, data=form) as resp:
            if resp.status != 200:
                error = await resp.text()
                raise Exception(f"Sarvam STT error {resp.status}: {error}")

            result = await resp.json()
            transcript = result.get("transcript", "")

        logger.info(f"Sarvam STT result: '{transcript}'")

        return stt.SpeechEvent(
            type=stt.SpeechEventType.FINAL_TRANSCRIPT,
            request_id=str(uuid.uuid4()),
            alternatives=[
                stt.SpeechData(
                    language=self._language,
                    text=transcript,
                    confidence=1.0,
                )
            ],
        )

    def _frame_to_wav(self, frame: rtc.AudioFrame) -> bytes:
        """Convert an AudioFrame to WAV bytes"""
        buf = io.BytesIO()
        with wave.open(buf, "wb") as wf:
            wf.setnchannels(frame.num_channels)
            wf.setsampwidth(2)  # 16-bit
            wf.setframerate(frame.sample_rate)
            wf.writeframes(bytes(frame.data))
        return buf.getvalue()

    async def aclose(self):
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None
