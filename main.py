# This file would contain the main function and capabilities of the toolkit


import json
import logging
import sys
import traceback
from typing import Dict, Any

from dynamic_template_llm_parser import DynamicTemplateLLMParser
from llm_parser import LLMParser

def handle_request(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Handle a toolkit request by routing to the appropriate parser capability.
    """
    capability = payload.get("capability")
    args = payload.get("args", {})
    
    if capability == "dynamic_template_llm_parser":
        config = args.get("config")
        source = args.get("source")
        
        if not config or not source:
            return {"error": "Missing required arguments: 'config' and 'source' are required", "capability": capability}
        
        try:
            parser = DynamicTemplateLLMParser(config, source)
            parser.run()
            return {"status": "success", "capability": capability, "message": "Dynamic Template LLM Parser executed."}
        except Exception as e:
            return {"error": str(e), "capability": capability, "traceback": traceback.format_exc()}

    elif capability == "llm_parser":
        config = args.get("config")
        source = args.get("source")
        
        if not config or not source:
            return {"error": "Missing required arguments: 'config' and 'source' are required", "capability": capability}
        
        try:
            parser = LLMParser(config, source)
            result_code = parser.process_file(config, source)
            
            status_map = {0: "success", 1: "failed", 2: "waiting"}
            status = status_map.get(result_code, "unknown")
            
            return {"status": status, "capability": capability, "result_code": result_code}
        except Exception as e:
            return {"error": str(e), "capability": capability, "traceback": traceback.format_exc()}
            
    else:
        return {"error": f"Unknown capability: {capability}", "capability": capability}

def main():
    # Silence all root logging to terminal if needed to ensure pure JSON output
    logging.getLogger().setLevel(logging.CRITICAL)
    for h in list(logging.root.handlers):
        logging.root.removeHandler(h)

    try:
        input_data = sys.stdin.read()
        if not input_data.strip():
            print(json.dumps({"error": "Empty input"}))
            sys.exit(1)
            
        payload = json.loads(input_data)
        response = handle_request(payload)
        print(json.dumps(response, indent=2))
        
    except json.JSONDecodeError as e:
        print(json.dumps({"error": f"Invalid JSON input: {str(e)}"}, indent=2))
        sys.exit(1)
    except Exception as e:
        print(json.dumps({"error": f"Unexpected error: {str(e)}"}, indent=2))
        sys.exit(1)

if __name__ == "__main__":
    main()