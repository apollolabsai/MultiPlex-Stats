"""
Form validation utilities.
"""
from typing import List, Dict, Any


def validate_server_config(data: Dict[str, Any]) -> List[str]:
    """
    Validate server configuration data.

    Args:
        data: Dictionary with 'name', 'ip_address', 'api_key', 'server_order'

    Returns:
        List of error messages (empty if valid)
    """
    errors = []

    # Validate name
    if not data.get('name') or not data['name'].strip():
        errors.append('Server name is required.')

    # Validate IP address
    if not data.get('ip_address') or not data['ip_address'].strip():
        errors.append('IP address is required.')
    elif ':' not in data['ip_address']:
        errors.append('IP address must include port (e.g., 192.168.1.100:8181).')

    # Validate API key
    if not data.get('api_key') or not data['api_key'].strip():
        errors.append('API key is required.')
    elif data['api_key'].strip().upper() == 'YOUR_API_KEY':
        errors.append('Please replace "YOUR_API_KEY" with your actual Tautulli API key.')

    return errors
