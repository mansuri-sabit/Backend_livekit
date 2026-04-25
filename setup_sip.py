"""
LiveKit SIP Trunk Setup Script

Run this once to configure LiveKit SIP trunks and dispatch rules.
After running, it will print the trunk IDs to add to your .env file.

Usage:
    python setup_sip.py

Prerequisites:
    - LIVEKIT_URL, LIVEKIT_API_KEY, LIVEKIT_API_SECRET in .env
    - SIP trunk provider credentials (Exotel SIP, Twilio, Telnyx, etc.)
"""
import os
import sys
import asyncio
import logging
from pathlib import Path
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


async def setup_sip_trunks():
    """Create SIP trunks and dispatch rules in LiveKit"""
    from livekit import api

    livekit_url = os.getenv("LIVEKIT_URL", "")
    api_key = os.getenv("LIVEKIT_API_KEY", "")
    api_secret = os.getenv("LIVEKIT_API_SECRET", "")

    if not all([livekit_url, api_key, api_secret]):
        logger.error("Missing LIVEKIT_URL, LIVEKIT_API_KEY, or LIVEKIT_API_SECRET in .env")
        sys.exit(1)

    lk = api.LiveKitAPI(url=livekit_url, api_key=api_key, api_secret=api_secret)

    sip_outbound_address = os.getenv("SIP_TRUNK_OUTBOUND_ADDRESS", "")
    sip_outbound_username = os.getenv("SIP_TRUNK_OUTBOUND_USERNAME", "")
    sip_outbound_password = os.getenv("SIP_TRUNK_OUTBOUND_PASSWORD", "")
    sip_outbound_number = os.getenv("SIP_TRUNK_OUTBOUND_NUMBER", "")

    try:
        # ── List existing trunks ──
        logger.info("Checking existing SIP trunks...")
        existing_outbound = await lk.sip.list_sip_outbound_trunk(api.ListSIPOutboundTrunkRequest())
        existing_inbound = await lk.sip.list_sip_inbound_trunk(api.ListSIPInboundTrunkRequest())
        existing_rules = await lk.sip.list_sip_dispatch_rule(api.ListSIPDispatchRuleRequest())

        if existing_outbound.items:
            logger.info(f"Found {len(existing_outbound.items)} existing outbound trunk(s):")
            for t in existing_outbound.items:
                logger.info(f"  - {t.name} (ID: {t.sip_trunk_id}, Address: {t.address})")

        if existing_inbound.items:
            logger.info(f"Found {len(existing_inbound.items)} existing inbound trunk(s):")
            for t in existing_inbound.items:
                logger.info(f"  - {t.name} (ID: {t.sip_trunk_id})")

        if existing_rules.items:
            logger.info(f"Found {len(existing_rules.items)} existing dispatch rule(s):")
            for r in existing_rules.items:
                logger.info(f"  - {r.name} (ID: {r.sip_dispatch_rule_id})")

        # ── Create Outbound Trunk (for making calls) ──
        outbound_trunk_id = os.getenv("SIP_TRUNK_OUTBOUND_ID", "")
        if not outbound_trunk_id:
            if not sip_outbound_address:
                logger.warning("SIP_TRUNK_OUTBOUND_ADDRESS not set. Skipping outbound trunk creation.")
                logger.info("To make outgoing calls, set your SIP provider credentials in .env:")
                logger.info("  SIP_TRUNK_OUTBOUND_ADDRESS=sip.your-provider.com")
                logger.info("  SIP_TRUNK_OUTBOUND_USERNAME=your_username")
                logger.info("  SIP_TRUNK_OUTBOUND_PASSWORD=your_password")
                logger.info("  SIP_TRUNK_OUTBOUND_NUMBER=+917941056502")
            else:
                logger.info(f"Creating outbound SIP trunk → {sip_outbound_address}...")
                outbound_trunk = await lk.sip.create_sip_outbound_trunk(
                    api.CreateSIPOutboundTrunkRequest(
                        trunk=api.SIPOutboundTrunkInfo(
                            name="Exotel Outbound",
                            address=sip_outbound_address,
                            numbers=[sip_outbound_number] if sip_outbound_number else [],
                            auth_username=sip_outbound_username,
                            auth_password=sip_outbound_password,
                        )
                    )
                )
                outbound_trunk_id = outbound_trunk.sip_trunk_id
                logger.info(f"Outbound trunk created: {outbound_trunk_id}")
        else:
            logger.info(f"Outbound trunk already configured: {outbound_trunk_id}")

        # ── Create Inbound Trunk (for receiving calls) ──
        inbound_trunk_id = os.getenv("SIP_TRUNK_INBOUND_ID", "")
        if not inbound_trunk_id:
            logger.info("Creating inbound SIP trunk...")
            inbound_trunk = await lk.sip.create_sip_inbound_trunk(
                api.CreateSIPInboundTrunkRequest(
                    trunk=api.SIPInboundTrunkInfo(
                        name="Exotel Inbound",
                        numbers=[sip_outbound_number] if sip_outbound_number else [],
                    )
                )
            )
            inbound_trunk_id = inbound_trunk.sip_trunk_id
            logger.info(f"Inbound trunk created: {inbound_trunk_id}")
        else:
            logger.info(f"Inbound trunk already configured: {inbound_trunk_id}")

        # ── Create Dispatch Rule (routes incoming SIP calls to rooms) ──
        if not existing_rules.items:
            logger.info("Creating SIP dispatch rule...")
            dispatch_rule = await lk.sip.create_sip_dispatch_rule(
                api.CreateSIPDispatchRuleRequest(
                    rule=api.SIPDispatchRuleInfo(
                        name="Route calls to agent",
                        trunk_ids=[inbound_trunk_id] if inbound_trunk_id else [],
                        rule=api.SIPDispatchRule(
                            dispatch_rule_individual=api.SIPDispatchRuleIndividual(
                                room_prefix="sip_",
                            )
                        ),
                    )
                )
            )
            logger.info(f"Dispatch rule created: {dispatch_rule.sip_dispatch_rule_id}")
        else:
            logger.info("Dispatch rule already exists, skipping.")

        # ── Print summary ──
        print("\n" + "=" * 60)
        print("SIP TRUNK SETUP COMPLETE")
        print("=" * 60)
        if outbound_trunk_id:
            print(f"\nAdd these to your .env file:")
            print(f"  SIP_TRUNK_OUTBOUND_ID={outbound_trunk_id}")
        if inbound_trunk_id:
            print(f"  SIP_TRUNK_INBOUND_ID={inbound_trunk_id}")
        print(f"\nLiveKit SIP endpoint for inbound calls:")
        print(f"  Point your SIP provider to route calls to LiveKit Cloud's SIP endpoint.")
        print(f"  Check LiveKit dashboard for your SIP URI.")
        print("=" * 60)

    except Exception as e:
        logger.error(f"Error setting up SIP trunks: {e}")
        import traceback
        traceback.print_exc()
    finally:
        await lk.aclose()


def main():
    logger.info("=" * 60)
    logger.info("LiveKit SIP Trunk Setup")
    logger.info("=" * 60)

    load_dotenv()

    asyncio.run(setup_sip_trunks())


if __name__ == "__main__":
    main()
