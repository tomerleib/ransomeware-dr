"""Configuration utilities for Pulumi programs."""

from typing import Dict, List, Optional, Any
from components.shared_imports import Config

def get_config(namespace: Optional[str] = None, required_fields: Optional[List[str]] = None, config_key: str = "serviceaccount") -> Dict[str, Any]:
    """
    Get configuration values for a given namespace or configuration object.
    
    Args:
        namespace: Optional namespace for the configuration (e.g., 'gcp')
        required_fields: Optional list of required fields to validate
        config_key: The configuration object key to use when namespace is None (default: 'serviceaccount')
    
    Returns:
        Dictionary containing configuration values
    
    Raises:
        ValueError: If any required fields are missing
    """
    if namespace:
        # For namespaced configs (e.g., gcp:region)
        config = Config(namespace)
        if required_fields:
            return {field: config.require(field) for field in required_fields}
        return {k: v for k, v in config.items()}
    else:
        # For non-namespaced config objects (e.g., serviceaccount, pubsub, etc.)
        config = Config()
        if required_fields:
            result = config.require_object(config_key)
            missing_fields = [field for field in required_fields if field not in result]
            if missing_fields:
                raise ValueError(f"Missing required configuration fields: {', '.join(missing_fields)}")
            return result
        return config.require_object(config_key) 
