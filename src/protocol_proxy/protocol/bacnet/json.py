import ipaddress
import json

from bacpypes3.apdu import ErrorRejectAbortNack, AbortPDU, ErrorPDU, RejectPDU


# TODO: Check how bacpypes3.json.sequence_to_json might be used in this.
# TODO: When can we handle an error, rather than just serializing it?
def serialize(val):
    """Helper method to handle BACnet responses and convert errors to JSON-serializable format."""
    if isinstance(val, AbortPDU):
        ret_val = {
            "error": "AbortPDU",
            "reason": str(val.apduAbortRejectReason) if hasattr(val, 'apduAbortRejectReason') else "Unknown abort reason",
            "details": str(val)
        }
    elif isinstance(val, ErrorPDU):
        ret_val = {
            "error": "ErrorPDU",
            "error_class": str(val.errorClass) if hasattr(val, 'errorClass') else "Unknown",
            "error_code": str(val.errorCode) if hasattr(val, 'errorCode') else "Unknown",
            "details": str(val)
        }
    elif isinstance(val, RejectPDU):
        ret_val = {
            "error": "RejectPDU",
            "reason": str(val.apduAbortRejectReason) if hasattr(val, 'apduAbortRejectReason') else "Unknown reject reason",
            "details": str(val)
        }
    elif isinstance(val, ErrorRejectAbortNack):
        ret_val = {
            "error": "ErrorRejectAbortNack",
            "details": str(val)
        }
    elif hasattr(val, '__class__') and 'Error' in val.__class__.__name__:
        ret_val = {
            "error": val.__class__.__name__,
            "details": str(val)
        }
    elif isinstance(val, (str, int, float, bool)):
        ret_val = val
    elif isinstance(val, (list, tuple, set)):
        ret_val = [serialize(v) for v in val]
    elif isinstance(val, (bytes, bytearray)):
        ret_val = val.hex()
    elif hasattr(val, '__dict__') and not isinstance(val, type):
        ret_val = {str(k): serialize(v) for k, v in val.__dict__.items()}
    elif hasattr(val, 'as_tuple'):
        ret_val = str(val)
    elif isinstance(val, (ipaddress.IPv4Address, ipaddress.IPv6Address)):
        ret_val = str(val)
    else:
        # TODO: Replace this forced string conversion with proper BACnet object serialization
        ret_val = f"FORCED:{str(val)}"
    return json.dumps(ret_val).encode('utf8')
