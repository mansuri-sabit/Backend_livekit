"""AWS S3 service for uploading call recordings and generating presigned URLs."""

from datetime import datetime, timezone
from typing import Optional

import boto3
from botocore.exceptions import ClientError

from config import get_settings
from utils.logger import logger

_s3_client = None


def _get_client():
    """Lazy-init S3 client (re-used across calls)."""
    global _s3_client
    if _s3_client is not None:
        return _s3_client

    s = get_settings()
    if not s.AWS_ACCESS_KEY_ID or not s.AWS_SECRET_ACCESS_KEY:
        raise RuntimeError("AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY must be set in .env")

    _s3_client = boto3.client(
        "s3",
        region_name=s.AWS_REGION,
        aws_access_key_id=s.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=s.AWS_SECRET_ACCESS_KEY,
    )
    return _s3_client


def _recording_key(call_id: str) -> str:
    """S3 object key: recordings/{YYYY}/{MM}/{call_id}.mp3"""
    now = datetime.now(timezone.utc)
    return f"recordings/{now.year}/{now.month:02d}/{call_id}.mp3"


def upload_recording(call_id: str, audio_buffer: bytes, content_type: str = "audio/mpeg") -> dict:
    """
    Upload audio buffer to S3.
    Returns: {"key": str, "bucket": str, "location": str}
    """
    s = get_settings()
    client = _get_client()
    key = _recording_key(call_id)

    client.put_object(
        Bucket=s.AWS_S3_BUCKET,
        Key=key,
        Body=audio_buffer,
        ContentType=content_type,
        Metadata={
            "callId": call_id,
            "uploadedAt": datetime.now(timezone.utc).isoformat(),
        },
    )
    location = f"https://{s.AWS_S3_BUCKET}.s3.{s.AWS_REGION}.amazonaws.com/{key}"
    logger.info(f"[S3] Uploaded recording: {key} ({len(audio_buffer)} bytes)")
    return {"key": key, "bucket": s.AWS_S3_BUCKET, "location": location}


def get_presigned_url(s3_key: str) -> Optional[str]:
    """Generate a presigned GET URL for the recording."""
    s = get_settings()
    client = _get_client()
    try:
        url = client.generate_presigned_url(
            "get_object",
            Params={"Bucket": s.AWS_S3_BUCKET, "Key": s3_key},
            ExpiresIn=s.AWS_S3_PRESIGNED_URL_EXPIRY,
        )
        return url
    except ClientError as e:
        logger.error(f"[S3] Presigned URL error for {s3_key}: {e}")
        return None


def delete_recording(s3_key: str) -> bool:
    """Delete a recording from S3."""
    s = get_settings()
    client = _get_client()
    try:
        client.delete_object(Bucket=s.AWS_S3_BUCKET, Key=s3_key)
        logger.info(f"[S3] Deleted recording: {s3_key}")
        return True
    except ClientError as e:
        logger.error(f"[S3] Delete error for {s3_key}: {e}")
        return False


def upload_voice_sample(provider: str, voice_id: str, audio_buffer: bytes, content_type: str = "audio/wav") -> dict:
    """
    Upload a static voice preview sample to S3.
    S3 key: voice-samples/{provider}/{voice_id}.wav
    """
    s = get_settings()
    client = _get_client()
    provider_safe = (provider or "").strip().lower() or "unknown"
    voice_safe = (voice_id or "").strip().lower() or "default"
    key = f"voice-samples/{provider_safe}/{voice_safe}.wav"

    client.put_object(
        Bucket=s.AWS_S3_BUCKET,
        Key=key,
        Body=audio_buffer,
        ContentType=content_type,
        Metadata={
            "provider": provider_safe,
            "voiceId": voice_safe,
            "uploadedAt": datetime.now(timezone.utc).isoformat(),
        },
    )
    location = f"https://{s.AWS_S3_BUCKET}.s3.{s.AWS_REGION}.amazonaws.com/{key}"
    logger.info(f"[S3] Uploaded voice sample: {key} ({len(audio_buffer)} bytes)")
    return {"key": key, "bucket": s.AWS_S3_BUCKET, "location": location}


def get_voice_sample_presigned_url(provider: str, voice_id: str) -> Optional[str]:
    """
    Return a presigned URL for a cached voice sample if it exists in S3.
    Returns None on cache miss.
    """
    s = get_settings()
    client = _get_client()
    provider_safe = (provider or "").strip().lower() or "unknown"
    voice_safe = (voice_id or "").strip().lower() or "default"
    key = f"voice-samples/{provider_safe}/{voice_safe}.wav"

    try:
        client.head_object(Bucket=s.AWS_S3_BUCKET, Key=key)
    except ClientError as e:
        error_code = str(e.response.get("Error", {}).get("Code", ""))
        if error_code in ("404", "NoSuchKey", "NotFound"):
            return None
        logger.error(f"[S3] head_object error for voice sample {key}: {e}")
        return None

    try:
        url = client.generate_presigned_url(
            "get_object",
            Params={"Bucket": s.AWS_S3_BUCKET, "Key": key},
            ExpiresIn=s.AWS_S3_PRESIGNED_URL_EXPIRY,
        )
        return url
    except ClientError as e:
        logger.error(f"[S3] Presigned URL error for voice sample {key}: {e}")
        return None
