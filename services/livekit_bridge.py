"""
LiveKit Audio Bridge

Bridges audio between Exotel WebSocket and a LiveKit room.
- Receives PCM audio from Exotel → publishes into LiveKit room as a participant
- Subscribes to the LiveKit Agent's audio → queues 20ms frames for Exotel

Audio quality design:
- Exotel format: 8kHz 16-bit mono PCM (320 bytes = 20ms)
- LiveKit format: 48kHz 16-bit mono
- Outbound frames are exactly 320 bytes (20ms) sent via async queue
  to avoid WebSocket contention and ensure smooth playback
"""
import asyncio
import base64
import json
import logging
import struct
import time
from array import array
from typing import Optional

from livekit import rtc, api

logger = logging.getLogger(__name__)

# Exotel audio: 8kHz 16-bit mono PCM
EXOTEL_SAMPLE_RATE = 8000
EXOTEL_CHANNELS = 1
EXOTEL_SAMPLE_WIDTH = 2  # 16-bit

# LiveKit audio: 48kHz 16-bit mono (LiveKit standard)
LIVEKIT_SAMPLE_RATE = 48000
LIVEKIT_CHANNELS = 1

# 20ms frame at 8kHz mono 16-bit = 160 samples × 2 bytes = 320 bytes
FRAME_DURATION_MS = 20
FRAME_BYTES = int(EXOTEL_SAMPLE_RATE * EXOTEL_SAMPLE_WIDTH * FRAME_DURATION_MS / 1000)  # 320
FRAME_INTERVAL_S = FRAME_DURATION_MS / 1000.0  # 0.02


class LiveKitBridge:
    """
    Bridges audio between an Exotel WebSocket and a LiveKit room.

    Uses an asyncio.Queue for outbound audio so the WebSocket sender
    runs independently from the LiveKit audio subscriber.
    """

    def __init__(
        self,
        livekit_url: str,
        api_key: str,
        api_secret: str,
    ):
        self.livekit_url = livekit_url
        self.api_key = api_key
        self.api_secret = api_secret

        self._room: Optional[rtc.Room] = None
        self._audio_source: Optional[rtc.AudioSource] = None
        self._local_track: Optional[rtc.LocalAudioTrack] = None
        self._stream_sid: str = ""
        self._call_sid: str = ""
        self._running: bool = False

        # Queue of pre-encoded JSON strings ready to send to Exotel WS
        self._send_queue: asyncio.Queue[str] = asyncio.Queue(maxsize=500)

        # PCM remainder buffer for slicing into exact 320-byte frames
        self._outbound_buf = bytearray()
        # Lock protecting _outbound_buf against concurrent mutation.
        # If a second audio track is subscribed (e.g. agent reconnects mid-call),
        # a second _forward_agent_audio task spawns. Both tasks would extend and
        # slice the same bytearray without a lock, interleaving PCM frames and
        # producing garbled audio at Exotel. asyncio.Lock() is sufficient because
        # both tasks run on the single asyncio event loop thread — no OS threads
        # are involved, so threading.Lock is not needed.
        self._outbound_buf_lock = asyncio.Lock()

    @property
    def running(self) -> bool:
        return self._running

    async def start(self, room_name: str) -> None:
        """
        Join LiveKit room as "phone_caller" participant.
        Does NOT take a websocket ref — outbound audio goes to the queue.
        """
        self._outbound_buf.clear()
        self._running = True

        # Generate a token for the caller participant
        token = api.AccessToken(self.api_key, self.api_secret)
        token.with_identity("phone_caller")
        token.with_name("Phone Caller")
        token.with_grants(
            api.VideoGrants(
                room_join=True,
                room=room_name,
                can_publish=True,
                can_subscribe=True,
                can_publish_sources=["microphone"],
            )
        )
        jwt_token = token.to_jwt()

        # Create audio source for publishing Exotel audio into LiveKit
        self._audio_source = rtc.AudioSource(
            sample_rate=LIVEKIT_SAMPLE_RATE,
            num_channels=LIVEKIT_CHANNELS,
        )

        self._local_track = rtc.LocalAudioTrack.create_audio_track(
            "phone_audio", self._audio_source
        )

        self._room = rtc.Room()

        # Listen for agent audio tracks
        @self._room.on("track_subscribed")
        def on_track_subscribed(
            track: rtc.Track,
            publication: rtc.RemoteTrackPublication,
            participant: rtc.RemoteParticipant,
        ):
            logger.info(
                f"Track subscribed: participant={participant.identity}, "
                f"kind={track.kind}, sid={track.sid}"
            )
            if track.kind == rtc.TrackKind.KIND_AUDIO:
                logger.info(
                    f"Starting audio forwarding from {participant.identity}"
                )
                # [DEBUG-a] Audio track received from remote participant
                logger.info(
                    f"[BRIDGE-DEBUG] Audio track received: "
                    f"participant={participant.identity} "
                    f"track_sid={track.sid} "
                    f"stream_sid={self._stream_sid}"
                )
                # SAFETY: @room.on callbacks are synchronous and may be fired
                # from a native/gRPC thread that has no asyncio event loop
                # attached. asyncio.create_task() raises RuntimeError in that
                # case, silently dropping the audio forwarding task so no audio
                # ever reaches Exotel.
                #
                # call_soon_threadsafe() is the correct cross-thread bridge:
                # it schedules the coroutine on the event loop from any thread,
                # even one with no running loop. ensure_future() is used instead
                # of create_task() because it works whether the loop is running
                # or not (create_task() requires a running loop).
                try:
                    loop = asyncio.get_event_loop()
                    loop.call_soon_threadsafe(
                        lambda t=track: asyncio.ensure_future(
                            self._forward_agent_audio(t)
                        )
                    )
                except RuntimeError as exc:
                    logger.error(
                        f"[Bridge] Could not schedule audio forwarding for "
                        f"participant={participant.identity}: {exc}. "
                        "Audio will not be forwarded for this track."
                    )

        await self._room.connect(self.livekit_url, jwt_token)
        logger.info(f"Bridge connected to LiveKit room: {room_name}")

        # Publish as SOURCE_MICROPHONE so the LiveKit agent recognizes it as speech input
        publish_opts = rtc.TrackPublishOptions()
        publish_opts.source = rtc.TrackSource.SOURCE_MICROPHONE
        await self._room.local_participant.publish_track(self._local_track, publish_opts)
        logger.info("Published caller audio track to LiveKit (source=MICROPHONE)")

    async def handle_exotel_message(self, raw_message: str) -> None:
        """Process an incoming WebSocket message from Exotel."""
        try:
            message = json.loads(raw_message)
            event = message.get("event", "")

            if event == "connected":
                logger.info("Exotel WebSocket connected (bridge mode)")

            elif event == "start":
                start_data = message.get("start", {})
                self._stream_sid = start_data.get("streamSid", "")
                self._call_sid = start_data.get(
                    "callSid", start_data.get("call_sid", "")
                )
                media_format = start_data.get(
                    "mediaFormat", start_data.get("media_format", {})
                )
                logger.info(
                    f"Stream started - SID: {self._stream_sid}, "
                    f"Call: {self._call_sid}, Format: {media_format}"
                )

            elif event == "media":
                media_data = message.get("media", {})
                payload = media_data.get("payload", "")
                if payload and self._audio_source:
                    audio_bytes = base64.b64decode(payload)
                    await self._publish_to_livekit(audio_bytes)

            elif event == "mark":
                logger.debug(f"Mark: {message.get('mark', {}).get('name', '')}")

            elif event == "dtmf":
                logger.info(f"DTMF: {message.get('dtmf', {}).get('digit', '')}")

            elif event == "stop":
                reason = message.get("stop", {}).get("reason", "unknown")
                logger.info(f"Stream stopped - Reason: {reason}")
                self._running = False

        except json.JSONDecodeError:
            logger.error(f"Invalid JSON: {raw_message[:100]}")
        except Exception as e:
            logger.error(f"Error handling message: {e}")

    # ── Inbound: Exotel 8kHz → LiveKit 48kHz ─────────────────────────

    async def _publish_to_livekit(self, pcm_8khz: bytes) -> None:
        """Upsample 8kHz PCM from Exotel to 48kHz and publish to LiveKit."""
        try:
            upsampled = self._upsample_pcm(pcm_8khz, 6)

            num_samples = len(upsampled) // EXOTEL_SAMPLE_WIDTH
            frame = rtc.AudioFrame(
                data=upsampled,
                sample_rate=LIVEKIT_SAMPLE_RATE,
                num_channels=LIVEKIT_CHANNELS,
                samples_per_channel=num_samples,
            )
            await self._audio_source.capture_frame(frame)
        except Exception as e:
            logger.error(f"Error publishing to LiveKit: {e}")

    @staticmethod
    def _upsample_pcm(pcm_data: bytes, factor: int) -> bytes:
        """
        Upsample 16-bit PCM by integer factor using linear interpolation.
        Uses array module for speed instead of per-sample Python loop.
        """
        num_samples = len(pcm_data) // 2
        if num_samples < 2:
            return pcm_data

        src = array("h")
        src.frombytes(pcm_data)

        out = array("h", [0]) * (num_samples * factor)

        for i in range(num_samples - 1):
            s0 = src[i]
            diff = src[i + 1] - s0
            base = i * factor
            for j in range(factor):
                out[base + j] = s0 + diff * j // factor

        # Last sample repeated
        base = (num_samples - 1) * factor
        last = src[-1]
        for j in range(factor):
            out[base + j] = last

        return out.tobytes()

    # ── Outbound: LiveKit agent → 20ms frames → Exotel queue ─────────

    async def _forward_agent_audio(self, track: rtc.Track) -> None:
        """
        Subscribe to agent's audio track at 8kHz, slice into 20ms frames
        (320 bytes each), and enqueue for the WebSocket sender.
        """
        logger.info("Starting agent audio forwarding to Exotel")

        audio_stream = rtc.AudioStream(
            track,
            sample_rate=EXOTEL_SAMPLE_RATE,
            num_channels=EXOTEL_CHANNELS,
        )

        frame_count = 0
        queued_count = 0
        last_frame_time: float | None = None

        try:
            async for frame_event in audio_stream:
                if not self._running:
                    break

                frame = frame_event.frame
                pcm_data = bytes(frame.data)
                if not pcm_data:
                    continue

                frame_count += 1

                now = time.monotonic()
                if last_frame_time is not None:
                    delta_ms = (now - last_frame_time) * 1000
                    if delta_ms > 50:
                        logger.warning(
                            f"Agent audio gap: {delta_ms:.1f}ms between frames "
                            f"(frame #{frame_count})"
                        )
                last_frame_time = now

                if frame_count <= 5 or frame_count % 100 == 0:
                    logger.info(
                        f"Agent audio frame #{frame_count}: "
                        f"{len(pcm_data)} bytes, "
                        f"{frame.sample_rate}Hz, "
                        f"{frame.num_channels}ch, "
                        f"streamSid='{self._stream_sid}'"
                    )

                # Accumulate and slice under the lock so a second concurrent
                # _forward_agent_audio task (spawned if the agent reconnects)
                # cannot interleave its PCM data into our 320-byte frame slices.
                async with self._outbound_buf_lock:
                    self._outbound_buf.extend(pcm_data)

                    while len(self._outbound_buf) >= FRAME_BYTES:
                        chunk = bytes(self._outbound_buf[:FRAME_BYTES])
                        del self._outbound_buf[:FRAME_BYTES]

                        payload = base64.b64encode(chunk).decode("utf-8")
                        msg = json.dumps({
                            "event": "media",
                            "streamSid": self._stream_sid,
                            "media": {"payload": payload},
                        })

                        try:
                            self._send_queue.put_nowait(msg)
                            queued_count += 1
                        except asyncio.QueueFull:
                            logger.warning(
                                f"Send queue overflow at frame #{frame_count} — "
                                f"dropping incoming frame. Queue size: {self._send_queue.qsize()}"
                            )

        except Exception as e:
            logger.error(f"Error forwarding agent audio: {e}", exc_info=True)
        finally:
            logger.info(
                f"Agent audio forwarding ended - "
                f"received {frame_count} frames, queued {queued_count} chunks"
            )

    async def drain_queue_to_ws(self, websocket) -> None:
        """
        Continuously drain the send queue to the Exotel WebSocket.
        Uses adaptive pacing anchored to a monotonic clock so accumulated
        processing time does not cause inter-frame drift.
        """
        sent_count = 0
        next_send_time: float | None = None

        try:
            while self._running:
                try:
                    msg = await asyncio.wait_for(
                        self._send_queue.get(), timeout=1.0
                    )
                except asyncio.TimeoutError:
                    continue

                # Anchor clock on first frame
                now = time.monotonic()
                if next_send_time is None:
                    next_send_time = now

                # Sleep until scheduled send time (skip if already behind)
                sleep_s = next_send_time - now
                if sleep_s > 0:
                    await asyncio.sleep(sleep_s)

                try:
                    await websocket.send_text(msg)
                    sent_count += 1
                    # [DEBUG-b] Bytes sent back to Exotel via WebSocket (first 10 frames only)
                    if sent_count <= 10:
                        logger.info(
                            f"[BRIDGE-DEBUG] Sent frame #{sent_count} to Exotel WS: "
                            f"{FRAME_BYTES} bytes, "
                            f"stream_sid={self._stream_sid} "
                            f"(queue depth={self._send_queue.qsize()})"
                        )
                except Exception as e:
                    logger.error(f"Error sending to Exotel WS: {e}")
                    self._running = False
                    break

                next_send_time += FRAME_INTERVAL_S

                # Drift correction: if >100ms behind real-time, reset clock
                if time.monotonic() - next_send_time > 0.100:
                    logger.warning(
                        f"drain_queue: >100ms behind real-time at frame #{sent_count}, "
                        f"resetting clock. Queue size: {self._send_queue.qsize()}"
                    )
                    next_send_time = time.monotonic()

                if sent_count <= 5 or sent_count % 100 == 0:
                    logger.info(
                        f"Sent frame #{sent_count} to Exotel WS "
                        f"(queue={self._send_queue.qsize()})"
                    )

        except Exception as e:
            logger.error(f"Queue drain error: {e}", exc_info=True)
        finally:
            logger.info(f"Queue drain ended — sent {sent_count} frames total")

    async def stop(self) -> None:
        """Disconnect from LiveKit and clean up."""
        self._running = False
        if self._room:
            await self._room.disconnect()
            logger.info("Bridge disconnected from LiveKit room")
        self._room = None
        self._audio_source = None
        self._local_track = None
        async with self._outbound_buf_lock:
            self._outbound_buf.clear()
