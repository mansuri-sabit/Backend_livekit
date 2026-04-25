"""
Sarvam AI TTS Plugin for LiveKit Agent

WebSocket streaming (bulbul:v3): one connection per synthesis stream, incremental
text from the LLM, audio chunks pushed as they arrive — no REST round-trip per
sentence.

Linear PCM at 16 kHz matches low-latency voice pipelines; LiveKit resamples as needed.
"""
from __future__ import annotations

import asyncio
import base64
import json
import logging
import uuid
from typing import ClassVar

import websockets
from livekit.agents import tts
from livekit.agents.types import APIConnectOptions

logger = logging.getLogger(__name__)

SARVAM_WS_URL = (
    "wss://api.sarvam.ai/text-to-speech/ws"
    "?model=bulbul:v3&send_completion_event=true"
)

# Pipeline output format (linear16 mono); matches Sarvam WebSocket pcm stream
SARVAM_SAMPLE_RATE = 16000

# LLM→TTS: buffer until major punctuation or this many chars (avoids comma/short-pause splits).
_TTS_TEXT_MAX_SEGMENT_CHARS = 150
_MAJOR_SENTENCE_END = frozenset(".!?;।")  # Latin + Devanagari danda (।)

# bulbul:v3 speakers accepted by the WebSocket API (do not include bulbul:v2-only names).
_VALID_SPEAKERS_V3 = frozenset({
    "aditya", "ritu", "ashutosh", "priya", "neha", "rahul", "pooja", "rohan", "simran",
    "kavya", "amit", "dev", "ishita", "shreya", "ratan", "varun", "manan", "sumit",
    "roopa", "kabir", "aayan", "shubh", "advait", "amelia", "sophia", "anand", "tanya",
    "tarun", "sunny", "mani", "gokul", "vijay", "shruti", "suhani", "mohit", "kavitha",
    "rehan", "soham", "rupali", "niharika",
})

_DEFAULT_SPEAKER = "priya"


def _should_send_text_chunk(raw: str) -> bool:
    """
    Sarvam bulbul:v3 rejects WebSocket text messages that are empty, whitespace-only,
    or have no characters from allowed languages. LLM streams can emit leading
    newlines or punctuation-only fragments — skip those.

    Uses strip() only for the empty check; the caller still sends the original
    chunk so spaces between streamed tokens are preserved.
    """
    if not (raw or "").strip():
        return False
    return any(ch.isalnum() for ch in raw)


def _pop_text_segment(buffer: str) -> tuple[str, str] | None:
    """
    Return (segment_to_send, new_buffer) if a segment is ready; else None.

    Splits only on major sentence punctuation (. ? ! ; ।), not commas — so the
    LLM stream is not chopped at clause boundaries. If no such break appears
    and len(buffer) >= _TTS_TEXT_MAX_SEGMENT_CHARS, split at a word boundary or
    hard at the limit.
    """
    if not buffer:
        return None
    for i, ch in enumerate(buffer):
        if ch in _MAJOR_SENTENCE_END:
            return (buffer[: i + 1], buffer[i + 1 :])
    if len(buffer) < _TTS_TEXT_MAX_SEGMENT_CHARS:
        return None
    window = buffer[:_TTS_TEXT_MAX_SEGMENT_CHARS]
    sp = window.rfind(" ")
    if sp > 20:
        cut = sp + 1
    else:
        cut = _TTS_TEXT_MAX_SEGMENT_CHARS
    return (buffer[:cut], buffer[cut:])


# bulbul:v2-only / legacy dashboard names → bulbul:v3 (must apply before _VALID_SPEAKERS_V3 check)
_SPEAKER_REMAP: dict[str, str] = {
    "anushka": "priya",
    "abhilash": "advait",
    "manisha": "kavya",
    "vidya": "ishita",
    "arya": "ritu",
    "karun": "rahul",
    "hitesh": "shubh",
}


def _resolve_speaker(voice: str) -> str:
    v = (voice or "").strip().lower()
    if v in _SPEAKER_REMAP:
        mapped = _SPEAKER_REMAP[v]
        logger.debug(
            "[SarvamTTS] Mapped legacy speaker %r → %r (bulbul:v3)",
            voice,
            mapped,
        )
        return mapped
    if v in _VALID_SPEAKERS_V3:
        return v
    logger.warning(
        "[SarvamTTS] Unknown speaker %r — using %r",
        voice,
        _DEFAULT_SPEAKER,
    )
    return _DEFAULT_SPEAKER


class TTS(tts.TTS):
    """Sarvam bulbul:v3 streaming TTS over WebSocket."""

    def __init__(
        self,
        *,
        api_key: str,
        language: str = "hi-IN",
        voice: str = "priya",
        pace: float = 1.2,
        min_buffer_size: int = 120,
        max_chunk_length: int = 150,
    ):
        super().__init__(
            capabilities=tts.TTSCapabilities(streaming=True, aligned_transcript=False),
            sample_rate=SARVAM_SAMPLE_RATE,
            num_channels=1,
        )
        self._api_key = api_key
        self._language = language
        self._voice = voice
        self._pace = pace
        self._min_buffer_size = min_buffer_size
        self._max_chunk_length = max_chunk_length

    @property
    def model(self) -> str:
        return "bulbul:v3"

    @property
    def provider(self) -> str:
        return "sarvam"

    def synthesize(self, text: str, *, conn_options=APIConnectOptions()):
        return self._synthesize_with_stream(text, conn_options=conn_options)

    def stream(self, *, conn_options=APIConnectOptions()):
        return _SarvamSynthesizeStream(
            tts=self,
            conn_options=conn_options,
        )

    async def aclose(self) -> None:
        pass


class _SarvamSynthesizeStream(tts.SynthesizeStream):
    """Single WebSocket session: stream LLM text in, stream linear16 audio out."""

    _tts_request_span_name: ClassVar[str] = "tts_sarvam_ws"

    def __init__(self, *, tts: TTS, conn_options: APIConnectOptions):
        super().__init__(tts=tts, conn_options=conn_options)
        self._tts: TTS = tts

    async def _run(self, output_emitter: tts.AudioEmitter) -> None:
        request_id = str(uuid.uuid4())
        segment_id = str(uuid.uuid4())
        speaker = _resolve_speaker(self._tts._voice)

        output_emitter.initialize(
            request_id=request_id,
            sample_rate=SARVAM_SAMPLE_RATE,
            num_channels=1,
            mime_type="audio/pcm",
            stream=True,
            frame_size_ms=20,
        )
        output_emitter.start_segment(segment_id=segment_id)

        headers = [("Api-Subscription-Key", self._tts._api_key)]

        # Pace is set on the opening `config` message (bulbul:v3 typically 0.5–2.0), not per text packet.
        config_payload = {
            "type": "config",
            "data": {
                "speaker": speaker,
                "target_language_code": self._tts._language,
                "pace": self._tts._pace,
                "speech_sample_rate": SARVAM_SAMPLE_RATE,
                "enable_preprocessing": False,
                "output_audio_codec": "linear16",
                "min_buffer_size": self._tts._min_buffer_size,
                "max_chunk_length": self._tts._max_chunk_length,
            },
        }

        try:
            async with websockets.connect(
                SARVAM_WS_URL,
                additional_headers=headers,
                max_size=16 * 1024 * 1024,
            ) as ws:
                await ws.send(json.dumps(config_payload))

                send_task = asyncio.create_task(self._send_loop(ws))
                recv_task = asyncio.create_task(self._recv_loop(ws, output_emitter))
                try:
                    await asyncio.gather(send_task, recv_task)
                finally:
                    for t in (send_task, recv_task):
                        if not t.done():
                            t.cancel()
                    await asyncio.gather(send_task, recv_task, return_exceptions=True)
        except Exception as exc:
            logger.exception("[SarvamTTS] WebSocket TTS failed: %s", exc)
            raise

    async def _send_loop(self, ws) -> None:
        """
        Buffer LLM token deltas; send larger text blocks to Sarvam only on major
        punctuation or length cap — avoids comma/whitespace-only flushes that
        cause choppy speech. Single-space chunks are kept in the buffer (not
        dropped) so words stay spaced.
        """
        buf = ""

        async def _emit_segment(text: str) -> None:
            if not _should_send_text_chunk(text):
                return
            await ws.send(json.dumps({"type": "text", "data": {"text": text}}))

        async def _flush_buffer_remainder() -> None:
            nonlocal buf
            t = buf
            buf = ""
            if not t:
                return
            await _emit_segment(t)

        try:
            async for data in self._input_ch:
                if isinstance(data, self._FlushSentinel):
                    await _flush_buffer_remainder()
                    await ws.send(json.dumps({"type": "flush"}))
                    continue
                raw = data if isinstance(data, str) else str(data)
                if raw == "":
                    continue
                buf += raw
                while True:
                    popped = _pop_text_segment(buf)
                    if popped is None:
                        break
                    segment, buf = popped
                    await _emit_segment(segment)
            await _flush_buffer_remainder()
            await ws.send(json.dumps({"type": "flush"}))
        except websockets.exceptions.ConnectionClosed:
            pass

    async def _recv_loop(self, ws, output_emitter: tts.AudioEmitter) -> None:
        while True:
            try:
                raw = await ws.recv()
            except websockets.exceptions.ConnectionClosed as exc:
                logger.debug("[SarvamTTS] WS closed: %s", exc)
                break

            msg = json.loads(raw) if isinstance(raw, str) else raw
            mtype = msg.get("type")

            if mtype == "audio":
                audio_b64 = (msg.get("data") or {}).get("audio") or msg.get("audio")
                if audio_b64:
                    pcm = base64.b64decode(audio_b64)
                    if pcm:
                        # Push PCM only; avoid flush() per tiny packet — that forces
                        # premature frame boundaries and can sound choppy. LiveKit's
                        # AudioByteStream emits frames from push() when enough samples
                        # accumulate; end_input on the stream flushes the rest.
                        output_emitter.push(pcm)
            elif mtype == "error":
                err = (msg.get("data") or {}).get("message") or msg
                raise RuntimeError(f"Sarvam TTS error: {err}")
            elif mtype == "event":
                ev = (msg.get("data") or {}).get("event_type")
                if ev == "final":
                    break
            elif mtype in ("ping", "pong"):
                continue
            else:
                logger.debug("[SarvamTTS] WS message (ignored): type=%s", mtype)
