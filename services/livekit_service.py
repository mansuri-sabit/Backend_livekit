"""
LiveKit Service - Handles LiveKit room management, token generation, and SIP calls

Changes from original:
  - create_room() now accepts an optional `metadata` dict and serialises it
    as JSON into the CreateRoomRequest.metadata field.  This is the mechanism
    used to pass tenant_id, campaign_id, and call_direction to the agent.
"""
import json
import logging
from typing import Optional
import livekit.api as api

logger = logging.getLogger(__name__)

# Reuse one LiveKit API client per (ws_url, key) so /voice-chat does not pay TLS + HTTP/2
# setup on every token request (~100–400ms saved toward a <2s browser handshake).
_lk_room_service_cache: dict[str, "LiveKitService"] = {}


def get_cached_livekit_service(api_key: str, api_secret: str, ws_url: str) -> "LiveKitService":
    """Return a process-wide cached LiveKitService for Room API calls. Do not close per request."""
    key = f"{ws_url}|{api_key}"
    svc = _lk_room_service_cache.get(key)
    if svc is None:
        svc = LiveKitService(api_key=api_key, api_secret=api_secret, ws_url=ws_url)
        _lk_room_service_cache[key] = svc
    return svc


class LiveKitService:
    """Service for managing LiveKit rooms, tokens, and SIP calls"""

    def __init__(self, api_key: str, api_secret: str, ws_url: str):
        self.api_key = api_key
        self.api_secret = api_secret
        self.ws_url = ws_url
        self._livekit_api = None

    async def _get_api(self):
        """Lazy initialization of LiveKit API"""
        if self._livekit_api is None:
            self._livekit_api = api.LiveKitAPI(
                url=self.ws_url,
                api_key=self.api_key,
                api_secret=self.api_secret
            )
        return self._livekit_api

    async def close(self):
        """Close the LiveKit API session"""
        if self._livekit_api is not None:
            await self._livekit_api.aclose()
            self._livekit_api = None

    async def create_room(
        self,
        room_name: str,
        empty_timeout: int = 300,
        metadata: dict | None = None,
    ) -> dict:
        """
        Create a new LiveKit room.

        Args:
            room_name:     Unique room name.
            empty_timeout: Seconds before an empty room is auto-deleted.
            metadata:      Optional dict serialised as JSON into room metadata.
                           The agent reads this to obtain tenant_id, campaign_id,
                           and call_direction without any external lookup.
        """
        metadata_str = json.dumps(metadata) if metadata else ""

        try:
            livekit_api = await self._get_api()
            # max_participants / empty_timeout cap room load (see LiveKit scaling docs).
            # Opus DTX, VP8/H.264 codec prefs, UDP ranges, and TURN are configured on
            # the LiveKit server (see config/livekit-server.example.yaml) or LiveKit Cloud
            # — not via this Python API.
            room = await livekit_api.room.create_room(
                api.CreateRoomRequest(
                    name=room_name,
                    empty_timeout=empty_timeout,
                    max_participants=10,
                    metadata=metadata_str,
                )
            )

            logger.info(
                f"Created LiveKit room: {room_name}"
                + (f" metadata={metadata}" if metadata else "")
            )
            return {
                "name": room.name,
                "sid": room.sid,
                "creation_time": room.creation_time,
            }
        except Exception as e:
            logger.error(f"Failed to create room '{room_name}': {e}")
            raise

    def generate_token(
        self,
        room_name: str,
        identity: str,
        name: Optional[str] = None,
        is_admin: bool = False
    ) -> str:
        """Generate access token for LiveKit room"""
        token = api.AccessToken(self.api_key, self.api_secret)
        token.with_identity(identity)
        token.with_name(name or identity)
        # VideoGrants = LiveKit SDK name for room permissions (audio + data, not "video only").
        token.with_grants(
            api.VideoGrants(
                room_join=True,
                room=room_name,
                can_publish=True,
                can_subscribe=True,
                can_publish_data=True,
                room_admin=is_admin,
                # Voice-only: do not allow publishing camera / screen (see also livekit_bridge).
                can_publish_sources=["microphone"],
            )
        )

        jwt_token = token.to_jwt()
        logger.info(f"Generated token for identity: {identity}")
        return jwt_token

    async def create_sip_participant(
        self,
        sip_trunk_id: str,
        phone_number: str,
        room_name: str,
        participant_identity: Optional[str] = None,
        participant_name: Optional[str] = None,
        play_dialtone: bool = True,
    ) -> dict:
        """
        Make an outgoing phone call via LiveKit SIP trunk.

        The phone call recipient becomes a participant in the LiveKit room.
        The agent (already in the room) can then talk to them in real-time.

        Args:
            sip_trunk_id: ID of the outbound SIP trunk
            phone_number: Phone number to call (e.g. +919307001740)
            room_name: LiveKit room name for the call
            participant_identity: Identity for the phone participant
            participant_name: Display name for the phone participant
            play_dialtone: Whether to play dialtone while connecting

        Returns:
            SIP participant info
        """
        try:
            livekit_api = await self._get_api()

            logger.info(f"Creating SIP participant: {phone_number} → room {room_name}")

            sip_participant = await livekit_api.sip.create_sip_participant(
                api.CreateSIPParticipantRequest(
                    sip_trunk_id=sip_trunk_id,
                    sip_call_to=phone_number,
                    room_name=room_name,
                    participant_identity=participant_identity or f"phone_{phone_number}",
                    participant_name=participant_name or f"Caller {phone_number}",
                    play_dialtone=play_dialtone,
                )
            )

            logger.info(f"SIP participant created: {sip_participant.participant_id}")
            return {
                "participant_id": sip_participant.participant_id,
                "participant_identity": sip_participant.participant_identity,
                "room_name": room_name,
                "sip_call_id": sip_participant.sip_call_id if hasattr(sip_participant, 'sip_call_id') else "",
            }

        except Exception as e:
            logger.error(f"Failed to create SIP participant: {e}")
            raise

    async def delete_room(self, room_name: str) -> bool:
        """Delete a LiveKit room"""
        try:
            livekit_api = await self._get_api()
            await livekit_api.room.delete_room(
                api.DeleteRoomRequest(room=room_name)
            )
            logger.info(f"Deleted room: {room_name}")
            return True
        except Exception as e:
            logger.error(f"Failed to delete room: {e}")
            return False
