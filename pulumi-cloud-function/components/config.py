"""Configuration utilities for Pulumi programs."""

from typing import Dict, List, Optional, Any
from components.shared_imports import Config

def get_config(namespace: Optional[str] = None, required_fields: Optional[List[str]] = None) -> Dict[str, Any]:
    """
    Get configuration values for a given namespace.
    
    Args:
        namespace: Optional namespace for the configuration (e.g., 'gcp', 'serviceaccount')
        required_fields: Optional list of required fields to validate
    
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
        # For non-namespaced configs (e.g., serviceaccount object)
        config = Config()
        if required_fields:
            result = config.require_object("serviceaccount")
            missing_fields = [field for field in required_fields if field not in result]
            if missing_fields:
                raise ValueError(f"Missing required configuration fields: {', '.join(missing_fields)}")
            return result
        return config.require_object("serviceaccount") 