# This file would contain the main function and capabilities of the toolkit

import json
import logging
import sys
from typing import Dict, Any

from dynamic_template_llm_parser import DynamicTemplateLLMParser
from llm_parser import LLMParser
from config import load_env

def handle_request(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Handle a toolkit request by routing to the appropriate parser capability.
    Standardized to return either 'result' or 'error' keys.
    """
    capability = payload.get("capability")
    args = payload.get("args", {})
    
    # dynamic template llm parser
    if capability == "dynamic_template_llm_parser":
        config = args.get("config")
        source = args.get("source")
        env_file = args.get("env_file")

        if not config or not source or not env_file:
            return {
                "error": "Missing required arguments: 'config', 'source', and 'env_file' are required",
                "capability": capability,
            }

        try:
            load_env(env_file)  # load client-specific env before anything else
        except FileNotFoundError as e:
            return {
                "error": str(e),
                "capability": capability,
            }

        try:
            parser = DynamicTemplateLLMParser(config, source)
            summary = parser.run()   # now returns _result_summary dict

            # ── Derive top-level status and message (REPORTING ONLY) ──────────────
            files = summary.get("files", [])
            fatal_error = summary.get("error")   # set only on validation/fatal failures

            if fatal_error:
                return {
                    "error": f"Execution failed before processing: {fatal_error}",
                    "capability": capability,
                }
            elif not files:
                return {
                    "error": "No supported files found to process.",
                    "capability": capability,
                }
            elif all(f.get("status") != "Completed" for f in files):
                first_error = next(
                    (f.get("error_message") for f in files if f.get("error_message")),
                    "All files failed — see 'files' for per-file details."
                )
                return {
                    "error": first_error,
                    "capability": capability,
                }
            elif all(f.get("status") == "Completed" for f in files):
                top_status  = "success"
                top_message = (
                    f"All {len(files)} file(s) processed successfully."
                )
            elif any(f.get("status") == "Completed" for f in files):
                completed = sum(1 for f in files if f.get("status") == "Completed")
                top_status  = "partial_success"
                top_message = (
                    f"{completed} of {len(files)} file(s) completed; "
                    f"see 'files' for per-file details."
                )

            # ── Collect every distinct error from failed files ───────────────────
            # Gives the caller a flat list without having to walk the files array
            file_errors = [
                {"file": f["file_name"], "error": f["error_message"]}
                for f in files
                if f.get("status") != "Completed" and f.get("error_message")
            ]

            return {
                "result": {
                    "status":  top_status,
                    "message": top_message,
                    # ── execution identifiers ──────────────────────────────────
                    "parser_file_execution_id": summary["parser_file_execution_id"],
                    "config_id":               summary["config_id"],
                    "source":                  summary["source"],
                    # ── timing ────────────────────────────────────────────────
                    "started_at":  summary["started_at"],
                    "finished_at": summary["finished_at"],
                    "duration_ms": summary["duration_ms"],
                    # ── file counters ──────────────────────────────────────────
                    "total_files":      summary["total_files"],
                    "skipped_files":    summary["skipped_files"],
                    "completed_files":  summary["completed_files"],
                    "failed_files":     summary["failed_files"],
                    "waiting_files":    summary["waiting_files"],
                    "unapproved_files": summary["unapproved_files"],
                    # ── errors (flat list — null when everything succeeded) ─────
                    # Non-null only when at least one file did not complete.
                    # Each entry: { "file": "<filename>", "error": "<reason>" }
                    "errors": file_errors or None,
                    # ── per-file detail ────────────────────────────────────────
                    # Each entry: file_name, file_path, status, vendor_text,
                    #             matched_vendor, matched_parser_id,
                    #             conversation_trace_id, qna_trace_id,
                    #             error_message, duration_ms
                    "files": summary["files"],
                },
                "capability": capability,
            }

        except Exception as e:
            return {
                "error": str(e),
                "capability": capability,
            }

    # llm parser
    elif capability == "llm_parser":
        config = args.get("config")
        source = args.get("source")
        env_file = args.get("env_file")

        if not config or not source or not env_file:
            return {
                "error": "Missing required arguments: 'config', 'source', and 'env_file' are required",
                "capability": capability,
            }

        try:
            load_env(env_file)  # load client-specific env before anything else
        except FileNotFoundError as e:
            return {
                "error": str(e),
                "capability": capability,
            }
    
        try:
            parser  = LLMParser(config, source)
            summary = parser.process_file(config, source)   # returns _result_summary dict
    
            result_code = summary.get("result_code", 1)
            status      = summary.get("status", "unknown")
    
            # On failure: surface the actual error, never a generic "executed" line
            if status == "success" or status == "waiting":
                message = summary.get("message")
            else:
                message = (
                    summary.get("error")
                    or summary.get("message")
                    or f"Parser failed with exit code {result_code}."
                )
                return {
                    "error": message,
                    "capability": capability,
                }
    
            return {
                "result": {
                    "status":      status,
                    "result_code": result_code,
                    "message":     message,
                    # ── execution identifiers ──────────────────────────────────
                    "parser_execution_id": summary.get("parser_execution_id"),
                    "parser_name":         summary.get("parser_name"),
                    "config_id":           summary.get("config_id"),
                    "source":              summary.get("source"),
                    # ── API trace ids ──────────────────────────────────────────
                    "qna_trace_id":  summary.get("qna_trace_id"),
                    # ── document stats ─────────────────────────────────────────
                    "total_pages":      summary.get("total_pages"),
                    "pages_processed":  summary.get("pages_processed"),
                    # ── DB insert result ───────────────────────────────────────
                    "records_processed": summary.get("records_processed"),
                    # ── timing ────────────────────────────────────────────────
                    "started_at":  summary.get("started_at"),
                    "finished_at": summary.get("finished_at"),
                    "duration_ms": summary.get("duration_ms"),
                    # ── error detail (null on success) ─────────────────────────
                    "error": summary.get("error") if status != "success" else None,
                },
                "capability": capability,
            }
    
        except Exception as e:
            return {
                "error": str(e),
                "capability": capability,
            }

def main():
    """Main entry point - reads JSON from stdin, outputs JSON to stdout"""
    # Silence all root logging to terminal to ensure pure JSON output
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
        print(json.dumps({
            "error": f"Invalid JSON input: {str(e)}",
            "capability": "unknown"
        }, indent=2))
        sys.exit(1)
    except Exception as e:
        print(json.dumps({
            "error": f"Unexpected error: {str(e)}",
            "capability": "unknown"
        }, indent=2))
        sys.exit(1)

if __name__ == "__main__":
    main()