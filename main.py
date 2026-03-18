# This file would contain the main function and capabilities of the toolkit

import json
import logging
import sys
import time
from datetime import datetime
from typing import Dict, Any
import os

from dynamic_template_llm_parser import DynamicTemplateLLMParser
from llm_parser import LLMParser

def checking_timeout(duration_minutes: float = None) -> Dict[str, Any]:
    """
    Loops and prints the current time every 10 seconds for a given duration.
    Defaults to 2 minutes if duration_minutes is not provided or empty.

    Args:
        duration_minutes: How long to run the loop (in minutes). Defaults to 2.

    Returns:
        A summary dict with total ticks and duration used.
    """
    # Default to 2 minutes if variable is empty/None
    if not duration_minutes:
        duration_minutes = 2

    duration_seconds = duration_minutes * 60
    interval_seconds = 10  # Print every 10 seconds
    start_time = time.time()
    tick = 0
    logs = []

    print(f"[checking_timeout] Starting loop for {duration_minutes} minute(s)...", flush=True)

    while True:
        elapsed = time.time() - start_time
        if elapsed >= duration_seconds:
            break

        tick += 1
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        message = f"[Tick {tick}] Current time: {current_time} | Elapsed: {elapsed:.1f}s / {duration_seconds}s"
        print(message, flush=True)
        logs.append(message)

        time.sleep(interval_seconds)

    summary_message = f"[checking_timeout] Done. Ran for {duration_minutes} minute(s), total ticks: {tick}"
    print(summary_message, flush=True)

    return {
        "duration_minutes": duration_minutes,
        "total_ticks": tick,
        "logs": logs,
        "summary": summary_message
    }

def checking_env(duration_minutes: float = None) -> Dict[str, Any]:
    """
    Prints all environment variables available to the process.
    """

    print("[checking_timeout] Listing environment variables:\n", flush=True)

    env_vars = {}

    for key, value in os.environ.items():
        print(f"{key} = {value}", flush=True)
        env_vars[key] = value

    print(f"\nTotal environment variables found: {len(env_vars)}", flush=True)

    return {
        "total_env_variables": len(env_vars),
        "environment_variables": env_vars
    }

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

        if not config or not source:
            return {
                "error": "Missing required arguments: 'config' and 'source' are required",
                "capability": capability,
            }

        try:
            summaries = DynamicTemplateLLMParser._start_processing(config, source)

            # ── Aggregate all per-instance summaries ──────────────────────────
            all_files = [fe for s in summaries for fe in s.get("files", [])]
            fatal_errors = [s.get("error") for s in summaries if s.get("error")]

            if not summaries or (fatal_errors and not all_files):
                return {
                    "error": fatal_errors[0] if fatal_errors else "No supported files found to process.",
                    "capability": capability,
                }

            non_result_statuses = {"Failed", "Waiting"}
            if all_files and all(f.get("status") in non_result_statuses for f in all_files):
                # Surface the actual per-file error messages
                file_errors = [
                    {"file": f["file_name"], "error": f["error_message"]}
                    for f in all_files
                    if f.get("error_message")
                ]
                first_error = file_errors[0]["error"] if file_errors else "All files failed — see 'files' for per-file details."
                return {
                    "error": first_error,
                    "capability": capability,
                    "files": all_files,  # always include per-file detail
                }

            completed   = sum(1 for f in all_files if f.get("status") == "Completed")
            unapproved  = sum(1 for f in all_files if f.get("status") == "Unapproved")
            resolved    = completed + unapproved   # both are valid terminal states

            if completed == len(all_files):
                top_status  = "success"
                top_message = f"All {len(all_files)} file(s) processed successfully."
            elif resolved == len(all_files) and unapproved > 0:
                top_status  = "unapproved"
                top_message = (
                    f"{unapproved} file(s) could not be matched to a vendor template. "
                    f"No data was extracted."
                )
            elif resolved > 0:
                top_status  = "partial_success"
                top_message = (
                    f"{completed} completed, {unapproved} unapproved, "
                    f"out of {len(all_files)} file(s); see 'files' for details."
                )
            else:
                top_status  = "partial_success"
                top_message = (
                    f"{completed} of {len(all_files)} file(s) completed; "
                    f"see 'files' for per-file details."
                )

            file_errors = [
                {"file": f["file_name"], "error": f["error_message"]}
                for f in all_files
                if f.get("status") != "Completed" and f.get("error_message")
            ]

            # For top-level identifiers use the first summary (single-file calls
            # have exactly one; directory calls produce one per file)
            first = summaries[0]

            return {
                "result": {
                    "status":  top_status,
                    "message": top_message,
                    # ── execution identifiers ──────────────────────────────────
                    "parser_file_execution_id": first["parser_file_execution_id"],
                    "config_id":               first["config_id"],
                    "source":                  first["source"],
                    # ── timing ────────────────────────────────────────────────
                    "started_at":  first["started_at"],
                    "finished_at": summaries[-1]["finished_at"],
                    "duration_ms": sum(s.get("duration_ms") or 0 for s in summaries),
                    # ── file counters ──────────────────────────────────────────
                    "total_files":      sum(s.get("total_files", 0)   for s in summaries),
                    "skipped_files":    sum(s.get("skipped_files", 0) for s in summaries),
                    "completed_files":  completed,
                    "failed_files":     sum(1 for f in all_files if f.get("status") == "Failed"),
                    "waiting_files":    sum(1 for f in all_files if f.get("status") == "Waiting"),
                    "unapproved_files": sum(1 for f in all_files if f.get("status") == "Unapproved"),
                    "records_processed": sum(f.get("records_processed") or 0 for f in all_files),
                    # ── errors (flat list — null when everything succeeded) ─────
                    "errors": file_errors or None,
                    # ── per-file detail ────────────────────────────────────────
                    "files": all_files,
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

        if not config or not source:
            return {
                "error": "Missing required arguments: 'config' and 'source' are required",
                "capability": capability,
            }

        try:
            summaries = LLMParser._start_processing(config, source)

            if not summaries:
                return {
                    "error": "No supported files found to process.",
                    "capability": capability,
                }

            # ── Single file — existing behaviour unchanged ─────────────────────
            if len(summaries) == 1:
                summary     = summaries[0]
                result_code = summary.get("result_code", 1)
                status      = summary.get("status", "unknown")

                if status not in ("success", "waiting"):
                    return {
                        "error": (
                            summary.get("error")
                            or summary.get("message")
                            or f"Parser failed with exit code {result_code}."
                        ),
                        "capability": capability,
                    }

                return {
                    "result": {
                        "status":              status,
                        "result_code":         result_code,
                        "message":             summary.get("message"),
                        "parser_execution_id": summary.get("parser_execution_id"),
                        "parser_name":         summary.get("parser_name"),
                        "config_id":           summary.get("config_id"),
                        "source":              summary.get("source"),
                        "qna_trace_id":        summary.get("qna_trace_id"),
                        "total_pages":         summary.get("total_pages"),
                        "pages_processed":     summary.get("pages_processed"),
                        "records_processed":   summary.get("records_processed"),
                        "inserted_records":    summary.get("inserted_records"),
                        "started_at":          summary.get("started_at"),
                        "finished_at":         summary.get("finished_at"),
                        "duration_ms":         summary.get("duration_ms"),
                        "error":               summary.get("error") if status != "success" else None,
                    },
                    "capability": capability,
                }

            # ── Directory / multiple files — aggregate summaries ───────────────
            completed = sum(1 for s in summaries if s.get("status") == "success")
            failed    = sum(1 for s in summaries if s.get("status") == "failed")
            waiting   = sum(1 for s in summaries if s.get("status") == "waiting")

            if completed == 0:
                first_error = next(
                    (s.get("error") or s.get("message") for s in summaries if s.get("status") == "failed"),
                    "All files failed.",
                )
                return {
                    "error": first_error,
                    "capability": capability,
                }

            top_status  = "success" if completed == len(summaries) else "partial_success"
            top_message = (
                f"All {len(summaries)} file(s) processed successfully."
                if top_status == "success"
                else f"{completed} of {len(summaries)} file(s) completed; see 'files' for per-file details."
            )

            file_errors = [
                {"file": os.path.basename(s.get("source", "")), "error": s.get("error") or s.get("message")}
                for s in summaries
                if s.get("status") not in ("success", "waiting") and (s.get("error") or s.get("message"))
            ]

            first = summaries[0]

            return {
                "result": {
                    "status":              top_status,
                    "message":             top_message,
                    "config_id":           first.get("config_id"),
                    "source":              source,
                    "started_at":          first.get("started_at"),
                    "finished_at":         summaries[-1].get("finished_at"),
                    "duration_ms":         sum(s.get("duration_ms") or 0 for s in summaries),
                    "total_files":         len(summaries),
                    "completed_files":     completed,
                    "failed_files":        failed,
                    "waiting_files":       waiting,
                    "records_processed":   sum(s.get("records_processed") or 0 for s in summaries),
                    "inserted_records":  [                                              # ADD
                        record
                        for s in summaries
                        for record in (s.get("inserted_records") or [])
                    ],
                    "errors":              file_errors or None,
                    "files":               summaries,
                },
                "capability": capability,
            }

        except Exception as e:
            return {
                "error": str(e),
                "capability": capability,
            }

    elif capability == "checking_timeout":
        # duration_minutes is optional — empty/missing defaults to 2 mins
        duration = args.get("duration_minutes", None)
        try:
            if duration is not None:
                duration = float(duration)
        except (ValueError, TypeError):
            return {"error": "Invalid value for duration_minutes. Must be a number."}

        result = checking_timeout(duration_minutes=duration)
        return {"result": result, "capability": capability}
    
    elif capability == "checking_env":
        result = checking_env()
        return {"result": result, "capability": capability}

    else:
        return {"error": f"Unknown capability: {capability}"}

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