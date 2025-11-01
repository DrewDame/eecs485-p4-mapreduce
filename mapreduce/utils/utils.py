import json

def dict_to_json(input_dict: dict) -> str:
    """Accepts a dictionary as input and returns a json string."""
    return json.dumps(input_dict)

def json_to_dict(input_json: str) -> dict:
    """Accepts a json string as input and returns a dictionary."""
    return json.loads(input_json)