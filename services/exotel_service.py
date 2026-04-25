"""
Exotel Service - Handles Exotel webhook events and call management
"""
import logging
from typing import Dict, Any, Optional
from xml.etree.ElementTree import Element, tostring
import aiohttp
import base64

logger = logging.getLogger(__name__)
outgoing_calls_logger = logging.getLogger("outgoing_calls")


class ExotelService:
    """Service for handling Exotel integrations"""
    
    def __init__(self, sid: str, api_key: str, auth_token: str, virtual_number: str, app_id: Optional[str] = None):
        self.sid = sid
        self.api_key = api_key
        self.auth_token = auth_token
        self.virtual_number = virtual_number
        self.app_id = app_id
        self.base_url = f"https://api.exotel.com/v1/Accounts/{sid}"
    
    def generate_connect_xml(self, livekit_sip_url: str) -> str:
        """
        Generate Exotel XML to connect call to LiveKit SIP
        
        Args:
            livekit_sip_url: SIP URL for LiveKit room
            
        Returns:
            XML string for Exotel response
        """
        response = Element('Response')
        
        # Add a welcome message before connecting
        say = Element('Say', {'voice': 'female'})
        say.text = "Hello! Aap Exotel AI voice system se connected hain. Main aapki madad kar sakta hoon."
        response.append(say)
        
        # Connect to LiveKit SIP
        dial = Element('Dial')
        number = Element('Number')
        number.text = livekit_sip_url
        dial.append(number)
        response.append(dial)
        
        xml_string = tostring(response, encoding='unicode')
        logger.info(f"Generated Exotel XML: {xml_string}")
        return xml_string
    
    def parse_webhook_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parse Exotel webhook payload
        
        Args:
            data: Webhook payload from Exotel
            
        Returns:
            Parsed call information
        """
        call_info = {
            "call_sid": data.get("CallSid", ""),
            "from_number": data.get("From", ""),
            "to_number": data.get("To", ""),
            "direction": data.get("Direction", ""),
            "status": data.get("Status", ""),
            "dial_call_duration": data.get("DialCallDuration", "0"),
        }
        
        logger.info(f"Parsed call info: {call_info}")
        return call_info
    
    def is_incoming_call(self, data: Dict[str, Any]) -> bool:
        """Check if the call is incoming"""
        return data.get("Direction", "").lower() == "incoming"
    
    def get_caller_number(self, data: Dict[str, Any]) -> str:
        """Get caller's phone number"""
        return data.get("From", "")
    
    def _get_auth_header(self) -> str:
        """Generate Basic Auth header for Exotel API"""
        credentials = f"{self.api_key}:{self.auth_token}"
        encoded = base64.b64encode(credentials.encode()).decode()
        return f"Basic {encoded}"
    
    def generate_greeting_xml(self, greeting_message: str) -> str:
        """
        Generate Exotel XML with greeting message
        
        Args:
            greeting_message: Message to speak when call connects
            
        Returns:
            XML string for Exotel
        """
        response = Element('Response')
        
        # Add the greeting message
        say = Element('Say', {'voice': 'female'})
        say.text = greeting_message
        response.append(say)
        
        # Add gather to listen for user response
        gather = Element('Gather', {
            'action': '/webhook/exotel/gather',
            'numDigits': '1',
            'timeout': '10'
        })
        gather_say = Element('Say', {'voice': 'female'})
        gather_say.text = "Please speak after the beep."
        gather.append(gather_say)
        response.append(gather)
        
        xml_string = tostring(response, encoding='unicode')
        logger.info(f"Generated greeting XML: {xml_string}")
        return xml_string
    
    async def make_outgoing_call(
        self, 
        to_number: str, 
        caller_id: Optional[str] = None,
        custom_field: Optional[str] = None,
        greeting_message: Optional[str] = None,
        webhook_url: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Make an outgoing call using Exotel API
        
        Args:
            to_number: Number to call (with country code)
            caller_id: Caller ID to display (defaults to virtual_number)
            custom_field: Custom data to pass with the call
            greeting_message: Message for AI to speak when call connects
            webhook_url: URL for Exotel to fetch JSON when call is answered
            
        Returns:
            API response with call details
        """
        # Exotel API endpoint for outgoing calls
        url = f"{self.base_url}/Calls/connect.json"
        logger.info(f"Exotel API URL: {url}")
        outgoing_calls_logger.info(f"API_CALL - URL: {url}, To: {to_number}")
        
        # Default greeting if not provided
        if not greeting_message:
            greeting_message = "Hello! This is an AI assistant speaking. This call has been generated using the Exotel AI voice system."
        
        # Use provided webhook URL or construct from app_id
        # The webhook_url should point to our /webhook/voicebot endpoint (JSON)
        if webhook_url:
            xml_url = webhook_url
        elif self.app_id:
            # Fallback to Exotel's app URL if no webhook provided
            xml_url = f"http://my.exotel.com/{self.sid}/exoml/start_voice/{self.app_id}"
        else:
            xml_url = ""
        
        logger.info(f"Exotel call payload - To: {to_number}, CallerId: {caller_id or self.virtual_number}, Url: {xml_url}")
        outgoing_calls_logger.info(f"PAYLOAD - To: {to_number}, CallerId: {caller_id or self.virtual_number}, Webhook: {xml_url}")
        
        # Build payload - only include Url if it's set
        payload = {
            "From": to_number,
            "CallerId": caller_id or self.virtual_number,
            "CustomField": greeting_message,
        }
        
        # Only add Url if we have a webhook URL
        if xml_url:
            payload["Url"] = xml_url
            logger.info(f"Using webhook URL: {xml_url}")
        else:
            logger.warning("No webhook URL provided - call will not connect to AI")
        
        headers = {
            "Authorization": self._get_auth_header(),
            "Content-Type": "application/x-www-form-urlencoded"
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, data=payload, headers=headers) as response:
                    result = await response.json()
                    logger.info(f"Outgoing call initiated: {result}")
                    call_sid = result.get("Call", {}).get("Sid", "N/A")
                    status = result.get("Call", {}).get("Status", "N/A")
                    outgoing_calls_logger.info(f"API_RESPONSE - CallSID: {call_sid}, Status: {status}, Response: {result}")
                    return result
        except Exception as e:
            logger.error(f"Failed to make outgoing call: {e}")
            outgoing_calls_logger.error(f"API_ERROR - To: {to_number}, Error: {str(e)}")
            return {"error": str(e)}
    
    async def get_call_details(self, call_sid: str) -> Dict[str, Any]:
        """
        Get details of a specific call
        
        Args:
            call_sid: Call SID to lookup
            
        Returns:
            Call details
        """
        url = f"{self.base_url}/Calls/{call_sid}.json"
        
        headers = {
            "Authorization": self._get_auth_header()
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers) as response:
                    result = await response.json()
                    return result
        except Exception as e:
            logger.error(f"Failed to get call details: {e}")
            return {"error": str(e)}
    
    async def hangup_call(self, call_sid: str) -> Dict[str, Any]:
        """
        Hangup an active call
        
        Args:
            call_sid: Call SID to hangup
            
        Returns:
            API response
        """
        url = f"{self.base_url}/Calls/{call_sid}.json"
        
        headers = {
            "Authorization": self._get_auth_header()
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.delete(url, headers=headers) as response:
                    result = await response.json()
                    logger.info(f"Call hung up: {call_sid}")
                    return result
        except Exception as e:
            logger.error(f"Failed to hangup call: {e}")
            return {"error": str(e)}
    
    async def download_recording(self, recording_url: str) -> bytes:
        """
        Download recording from Exotel
        
        Args:
            recording_url: URL of the recording
            
        Returns:
            Audio bytes
        """
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(recording_url) as response:
                    if response.status == 200:
                        audio_data = await response.read()
                        logger.info(f"Downloaded recording: {len(audio_data)} bytes")
                        return audio_data
                    else:
                        logger.error(f"Failed to download recording: {response.status}")
                        return b""
        except Exception as e:
            logger.error(f"Error downloading recording: {e}")
            return b""
