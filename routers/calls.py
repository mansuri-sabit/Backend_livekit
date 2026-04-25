"""
Calls Router - Handles outgoing call initiation via Exotel API
"""
import logging
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from typing import Optional

from config import get_settings, Settings
from services.exotel_service import ExotelService

logger = logging.getLogger(__name__)
outgoing_calls_logger = logging.getLogger("outgoing_calls")

router = APIRouter(prefix="/calls", tags=["calls"])


class OutgoingCallRequest(BaseModel):
    """Request model for outgoing call"""
    to_number: str
    greeting_message: Optional[str] = "Hello! This is an AI assistant speaking."


class OutgoingCallResponse(BaseModel):
    """Response model for outgoing call"""
    success: bool
    message: str
    call_sid: Optional[str] = None
    details: Optional[dict] = None


def get_exotel_service(settings: Settings = Depends(get_settings)) -> ExotelService:
    """Dependency to get Exotel service"""
    return ExotelService(
        sid=settings.EXOTEL_SID,
        api_key=settings.EXOTEL_API_KEY,
        auth_token=settings.EXOTEL_AUTH_TOKEN,
        virtual_number=settings.EXOTEL_VIRTUAL_NUMBER,
        app_id=settings.EXOTEL_APP_ID,
    )


@router.post("/outgoing", response_model=OutgoingCallResponse)
async def initiate_outgoing_call(
    request: OutgoingCallRequest,
    settings: Settings = Depends(get_settings),
    exotel_service: ExotelService = Depends(get_exotel_service),
):
    """
    Initiate an outgoing call via Exotel API.

    Flow:
    1. Exotel dials the phone number
    2. User picks up → Exotel Voicebot applet activates
    3. Voicebot hits /webhook/voicebot → gets WebSocket URL
    4. Voicebot connects via WebSocket → real-time AI conversation begins
    """
    try:
        if not request.to_number.startswith("+"):
            raise HTTPException(
                status_code=400,
                detail="Phone number must include country code (e.g., +919307001740)"
            )

        logger.info(f"Initiating outgoing call to: {request.to_number}")
        outgoing_calls_logger.info(f"INITIATE - To: {request.to_number}, Greeting: {request.greeting_message}")

        result = await exotel_service.make_outgoing_call(
            to_number=request.to_number,
            caller_id=settings.EXOTEL_VIRTUAL_NUMBER,
            custom_field=request.greeting_message,
            greeting_message=request.greeting_message,
            webhook_url=None,  # Uses Exotel App ID (Voicebot applet)
        )

        if "error" in result:
            logger.error(f"Failed to initiate call: {result['error']}")
            outgoing_calls_logger.error(f"FAILED - To: {request.to_number}, Error: {result['error']}")
            return OutgoingCallResponse(
                success=False,
                message=f"Failed to initiate call: {result['error']}",
                details=result,
            )

        call_sid = result.get("Call", {}).get("Sid", "")
        logger.info(f"Call initiated successfully. Call SID: {call_sid}")
        outgoing_calls_logger.info(f"SUCCESS - To: {request.to_number}, CallSID: {call_sid}")

        return OutgoingCallResponse(
            success=True,
            message=f"Call initiated to {request.to_number}",
            call_sid=call_sid,
            details=result,
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error initiating outgoing call: {e}")
        return OutgoingCallResponse(
            success=False,
            message=f"Error: {str(e)}",
        )


@router.get("/status/{call_sid}")
async def get_call_status(
    call_sid: str,
    exotel_service: ExotelService = Depends(get_exotel_service),
):
    """Get the status of a specific call"""
    try:
        result = await exotel_service.get_call_details(call_sid)
        return {"success": True, "call_sid": call_sid, "details": result}
    except Exception as e:
        logger.error(f"Error getting call status: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/test-outgoing")
async def test_outgoing_call_form():
    """Returns a simple HTML form to test outgoing calls"""
    html_content = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Test Outgoing Call - Exotel AI Voice</title>
        <style>
            body { font-family: Arial, sans-serif; max-width: 600px; margin: 50px auto; padding: 20px; }
            input, button { width: 100%; padding: 10px; margin: 10px 0; font-size: 16px; }
            button { background: #007bff; color: white; border: none; cursor: pointer; border-radius: 5px; }
            button:hover { background: #0056b3; }
            .result { margin-top: 20px; padding: 15px; background: #f0f0f0; border-radius: 5px; }
            h1 { color: #333; }
            .info { background: #e7f3ff; padding: 10px; border-radius: 5px; margin-bottom: 20px; font-size: 14px; }
        </style>
    </head>
    <body>
        <h1>Test Outgoing Call</h1>
        <div class="info">
            <strong>Architecture:</strong> Exotel Voicebot WebSocket<br>
            <strong>Flow:</strong> Exotel dials number &rarr; Voicebot connects via WebSocket &rarr; Real-time AI conversation
        </div>
        <form id="callForm">
            <label>Phone Number (with country code):</label>
            <input type="text" id="to_number" value="+919307001740" placeholder="+919307001740" required>
            <button type="submit">Initiate Call</button>
        </form>
        <div id="result" class="result" style="display:none;"></div>

        <script>
            document.getElementById('callForm').onsubmit = async (e) => {
                e.preventDefault();
                const resultDiv = document.getElementById('result');
                resultDiv.style.display = 'block';
                resultDiv.innerHTML = 'Initiating call...';

                try {
                    const response = await fetch('/calls/outgoing', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({
                            to_number: document.getElementById('to_number').value,
                        })
                    });
                    const data = await response.json();
                    resultDiv.innerHTML = '<pre>' + JSON.stringify(data, null, 2) + '</pre>';
                } catch (error) {
                    resultDiv.innerHTML = 'Error: ' + error.message;
                }
            };
        </script>
    </body>
    </html>
    """
    from fastapi.responses import HTMLResponse
    return HTMLResponse(content=html_content)
