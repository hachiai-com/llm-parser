"""
Python equivalent of DynamicTemplateLLMParser.java

Main implemented:
    - identify_vendor         : send doc to Conversation API, extract vendor text
    - match_vendor_parser     : match vendor text against DB, get parser class
    - update_execution_status : write status to DB, move files, cleanup

CLI:
    python dynamic_template_llm_parser.py --config <id> --source <path>
"""

import json
import os
import re
import shutil
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional
import requests
import concurrent.futures
from config import DBConfig, LLMConfig
from constants import MYSQL_DB, FileExecutionStatus, TaskExecutionStatus
from logger import get_static_logger, get_instance_logger, close_instance_logger
from sql_dao import SqlDao
from models.llm_config_bean import LLMConfigBean
from models.execution_data import ExecutionData
from models.task_detail import TaskDetail
from models.task_response import TaskResponse



static_logger = get_static_logger("DynamicTemplateLLMParser")
SUPPORTED_EXT_RE = re.compile(r"\.(pdf|png|jpg|jpeg)$", re.IGNORECASE)

class DynamicTemplateLLMParser:
    """
    Python equivalent of DynamicTemplateLLMParser.java

    Usage:
        parser = DynamicTemplateLLMParser(config_file_path, source_input)
        result = parser.run()   # now returns _result_summary dict

    CLI:
        python dynamic_template_llm_parser.py --config <id_or_file> --source <path>
    """

    def __init__(self, config_file_path: str, source_input: str) -> None:
        self.config_file_path = config_file_path
        self.source_input     = source_input
        self.parser_file_execution_id = str(uuid.uuid4())
        self.logger = get_instance_logger(self.parser_file_execution_id, "DynamicTemplateLLMParser")
        self.execution_data = ExecutionData(parserFileExecutionId=self.parser_file_execution_id)
        self.config: Optional[LLMConfigBean] = None
        self.dao:    Optional[SqlDao]         = None

        # ── REPORTING ONLY ────────────────────────────────────────────────────
        # These variables are populated during execution solely for returning a
        # rich result summary to the caller. They are never used for any logic.
        self._result_summary: dict = {
            "parser_file_execution_id": self.parser_file_execution_id,
            "config_id":   config_file_path,
            "source":      source_input,
            "files":       [],   # one entry per processed file (see _make_file_entry)
            "total_files":     0,
            "skipped_files":   0,
            "completed_files": 0,
            "failed_files":    0,
            "waiting_files":   0,
            "unapproved_files": 0,
            "started_at":  datetime.utcnow().isoformat() + "Z",
            "finished_at": None,
            "duration_ms": None,
            "error":       None,
        }
        self._result_summary_start_ms: float = time.time() * 1000  # REPORTING ONLY

    # ── REPORTING ONLY helper ─────────────────────────────────────────────────
    @staticmethod
    def _make_file_entry(file_path: str) -> dict:
        """
        REPORTING ONLY — creates the per-file dict that is appended to
        _result_summary["files"]. Nothing in this dict is read back for logic.
        """
        return {
            "file_name":              os.path.basename(file_path),
            "file_path":              file_path,
            "status":                 None,   # filled by _process_single_file
            "vendor_text":            None,   # filled after identify_vendor
            "matched_vendor":         None,   # filled after match_vendor_parser
            "matched_parser_id":      None,   # filled after match_vendor_parser
            "conversation_trace_id":  None,   # filled after identify_vendor
            "qna_trace_id":           None,   # filled after invoke_llm_parser
            "error_message":          None,   # filled on failure
            "duration_ms":            None,   # filled at end of _process_single_file
        }

    def _finalize_result_summary(self) -> None:
        """REPORTING ONLY — stamps finish time and aggregates file counters."""
        now_ms = time.time() * 1000
        self._result_summary["finished_at"] = datetime.utcnow().isoformat() + "Z"
        self._result_summary["duration_ms"] = round(now_ms - self._result_summary_start_ms)
        for entry in self._result_summary["files"]:
            s = entry.get("status")
            if s == str(FileExecutionStatus.Completed):
                self._result_summary["completed_files"] += 1
            elif s == str(FileExecutionStatus.Failed):
                self._result_summary["failed_files"] += 1
            elif s == str(FileExecutionStatus.Waiting):
                self._result_summary["waiting_files"] += 1
            elif s == str(FileExecutionStatus.Unapproved):
                self._result_summary["unapproved_files"] += 1

    # ── run ─────────────────────────────────────────────────────────────────
    def run(self) -> dict:
        """
        Executes the parser and returns _result_summary.
        The return value is REPORTING ONLY — callers can display it but must
        not use it to drive any further parsing logic.
        """
        try:
            self._execute_matched_parser(self.source_input)
        except Exception as ex:
            self.logger.error(f"Fatal error: {ex}", exc_info=True)
            self._result_summary["error"] = str(ex)  # REPORTING ONLY
        finally:
            if self.dao:
                self._destroy()
            self._finalize_result_summary()  # REPORTING ONLY
        return self._result_summary  # REPORTING ONLY

    def _resolve_config(self, config_input) -> Optional[str]:
        """
        Resolves config from three possible inputs and sets self.config.
        Returns an error string on failure, or None on success.
        """
        if isinstance(config_input, LLMConfigBean):
            self.logger.info("Config provided as LLMConfigBean — using directly.")
            self.config = config_input
            return None

        config_input = str(config_input).strip()

        if os.path.isfile(config_input):
            self.logger.info(f"Loading config from file: '{config_input}'")
            try:
                with open(config_input, "r", encoding="utf-8") as f:
                    self.config = LLMConfigBean(**json.load(f))
                if self.config.show_query:
                    self.logger.info(f"Config loaded: {self.config}")
                return None
            except Exception as ex:
                return f"Failed to load config file '{config_input}': {ex}"

        if config_input.isdigit():
            self.logger.info(f"Loading config from DB for parser id: '{config_input}'")
            rows = self._get_parser_config_from_db(config_input)
            if not rows:
                return f"No active config found in DB for parser id: '{config_input}'"
            try:
                self.logger.info(f"rows in resolve config={rows}")
                config_data = json.loads(rows[0]["config"])
                self.logger.info(f"config data: {config_data}")
                self.config = LLMConfigBean(**config_data)
                if self.config.show_query:
                    self.logger.info(f"Config loaded: {self.config}")
                return None
            except Exception as ex:
                return f"Failed to parse DB config for parser id '{config_input}': {ex}"

        return f"Config not recognised (not a file path, not a numeric id): '{config_input}'"

    def _get_parser_config_from_db(self, config_input: str) -> list:
        """Short-lived connection using .env DB creds."""
        tmp_dao = None
        try:
            tmp_dao = SqlDao(
                db_type   = DBConfig.type(),
                host      = DBConfig.host(),
                port      = DBConfig.port(),
                database  = DBConfig.database_name(),
                user_name = DBConfig.username(),
                password  = DBConfig.password(),
                logger    = self.logger,
            )
            return tmp_dao.run_query(
                "SELECT * FROM parser_config WHERE status = %s AND (id = %s OR name = %s)",
                ["active", config_input, config_input],
            )
        except Exception as ex:
            self.logger.error(f"getParserConfigFromDB failed: {ex}", exc_info=True)
            return []
        finally:
            if tmp_dao:
                tmp_dao.close()

    def _load_db(self) -> None:
        self.dao = SqlDao(
            db_type   = DBConfig.type(),
            host      = DBConfig.host(),
            port      = DBConfig.port(),
            database  = DBConfig.database_name(),
            user_name = DBConfig.username(),
            password  = DBConfig.password(),
            logger    = self.logger,
        )

    @staticmethod
    def _get_files_by_filter(directory: str) -> list:
        """Mirrors Util.getFilesByFilter()"""
        return [
            os.path.abspath(os.path.join(directory, name))
            for name in os.listdir(directory)
            if os.path.isfile(os.path.join(directory, name)) and SUPPORTED_EXT_RE.search(name)
        ]

    # ── identify_vendor ─────────────────────────────────────────────────────
    def identify_vendor(self, file_path: str) -> Optional[TaskResponse]:
        """
        identify_vendor  (1st LLM API call)
        Sends document to Conversation API, polls for result, returns full TaskResponse.
        Mirrors: DynamicTemplateLLMParser.getInputFileText()
        """
        self.logger.info(f"identify_vendor: file='{file_path}'")
        task_response = None
        try:
            task_detail = self._send_conversation_api_request(file_path)
            self.execution_data.conversationTraceId = task_detail.trace_id
            task_response = self._track_conversation_api_status(task_detail)
            self.logger.info(f"Conversation API status={task_response.status}")
        except Exception as ex:
            self.logger.error(f"identify_vendor failed: {ex}", exc_info=True)
        return task_response

    def _send_conversation_api_request(self, file_path: str) -> TaskDetail:
        """Mirrors: DynamicTemplateLLMParser.sendConversationAPIRequest()"""
        form_data = {
            "prompt":      LLMConfig.dynamic_parser_query(),
            "max_token":   LLMConfig.max_token(),
            "temperature": LLMConfig.temperature(),
            "tags":        json.dumps(["vendor-identifier"]),
        }
        response_text = self._post_with_file(
            url=LLMConfig.conversation_api(), token=LLMConfig.token(),
            file_path=file_path, form_data=form_data, timeout=LLMConfig.http_timeout(),
        )
        self.logger.info(f"Conversation API response: {response_text}")
        return self._parse_task_detail(response_text)

    def _track_conversation_api_status(self, task_detail: TaskDetail) -> TaskResponse:
        """Mirrors: DynamicTemplateLLMParser.trackConversationAPIStatus()"""
        total_iterations = LLMConfig.conv_total_iterations()
        interval_ms      = LLMConfig.conv_interval_seconds() * 1000
        if task_detail.wait_time_ms:
            interval_ms = int(task_detail.wait_time_ms)

        trace_id = task_detail.trace_id
        if not trace_id:
            raise ValueError("Trace ID missing in conversation API response.")

        task_response = TaskResponse()
        for attempt in range(1, total_iterations + 1):
            self.logger.info(f"Conversation API — TraceId:{trace_id} — Attempt #{attempt}: waiting {interval_ms}ms...")
            time.sleep(interval_ms / 1000)
            task_response = self._parse_task_response(self._get_status_by_trace_id(trace_id))

            if task_response.status.lower() == TaskExecutionStatus.fulfilled.value:
                self.logger.info(f"Conversation API — TraceId:{trace_id} — completed.")
                return task_response
            elif task_response.status.lower() == TaskExecutionStatus.failed.value:
                self.logger.info(f"Conversation API — TraceId:{trace_id} — failed.")
                return task_response

        self.logger.info(f"Conversation API — TraceId:{trace_id} — max iterations reached. Marking it as Failed.")
        task_response.status = FileExecutionStatus.waiting.value
        return task_response

    # ── match_vendor_parser ──────────────────────────────────────────────────
    def match_vendor_parser(self, vendor_text: str) -> dict:
        """
        match_vendor_parser
        Queries vendor reference table for a match, then fetches parser config.
        Mirrors: DynamicTemplateLLMParser.getVendorParserDetailIfMatchedInFile()
        """
        self.logger.info(f"match_vendor_parser: vendor_text='{vendor_text}'")
        self.logger.info(f"self.config {self.config}")
        rows    = self._get_select_query_results(f"SELECT * FROM {self.config.templateReferenceDBTable}")
        ref_col = self.config.templateReferenceDBColumn
        matched_vendor = None

        for row in rows:
            db_text = row.get(ref_col, "") or ""
            if db_text and vendor_text and vendor_text.lower() in db_text.lower():
                self.logger.info(f"Vendor matched: '{db_text}'")
                matched_vendor = row
                break

        if not matched_vendor:
            self.logger.info("No vendor match found.")
            return {"vendor_match": None, "parser_id": None, "config_class": None, "parser_type": None}

        parser_id = matched_vendor.get("parser_id", "")
        self.execution_data.matchedParserId = parser_id

        query = (
            "SELECT pc.id, pc.org_id, pc.parser_type, pct.bash_file, pct.config_class "
            "FROM uxploreaudit.parser_config pc "
            "LEFT JOIN uxploreaudit.parser_config_type pct ON pc.parser_type = pct.parser_type "
            f"WHERE pc.id = {parser_id};"
        )
        config_rows = self._get_select_query_results_via_parent_account(query)
        if not config_rows:
            self.logger.info(f"parser_id={parser_id} not found in parser_config.")
            return {"vendor_match": matched_vendor, "parser_id": parser_id,
                    "config_class": None, "parser_type": None}

        config_class = config_rows[0].get("config_class", "")
        parser_type  = config_rows[0].get("parser_type", "")
        self.logger.info(f"Matched — parser_id={parser_id}  config_class={config_class}  parser_type={parser_type}")
        return {"vendor_match": matched_vendor, "parser_id": parser_id,
                "config_class": config_class, "parser_type": parser_type}

    # ── update_execution_status ──────────────────────────────────────────────
    def update_execution_status(self, file_path: str, status: FileExecutionStatus) -> None:
        """
        update_execution_status  (always runs — mirrors Java finally block)
        1. Writes status row to DB.
        2. Moves file to configured directory.
        3. Closes DB + flushes log handlers.
        """
        self.logger.info(f"update_execution_status: status={status}  file='{file_path}'")
        self._update_file_status(file_path, status)
        if status in (FileExecutionStatus.Completed, FileExecutionStatus.Failed, FileExecutionStatus.Unapproved):
            self._move_file_based_on_status(file_path, status)
        # if self.dao:
        #     self._destroy()
        # close_instance_logger(self.logger)

    def _update_file_status(self, file_path: str, status: FileExecutionStatus) -> None:
        if not self.config:
            raise RuntimeError("Config not initialized")
        elif not self.config.fileParsingStatusReferenceDBTable:
            return

        total_pages = "0"
        pages_processed = "0"
        if self.execution_data.taskResponse is not None:
            total_pages = str(self.execution_data.taskResponse.total_pages)
            pages_processed = str(self.execution_data.taskResponse.pages_processed)

        query = (
            f"INSERT INTO {self.config.fileParsingStatusReferenceDBTable} "
            "(parser_file_execution_id, parser_id, parser_execution_id, file_name, file_status, "
            "conversation_api_trace_id, matched_parser_id, conversation_api_response, "
            "qna_api_trace_id, error_message, total_pages, pages_processed) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
            "ON DUPLICATE KEY UPDATE file_status = VALUES(file_status), date_updated = CURRENT_TIMESTAMP"
        )
        try:
            self.dao.exec_ins_query(query, [[
                self.execution_data.parserFileExecutionId,
                self.config_file_path,
                self._get_parser_execution_id(),
                os.path.basename(file_path),
                str(status),
                self.execution_data.conversationTraceId,
                self.execution_data.matchedParserId,
                self.execution_data.textFromConversationAPI,
                self.execution_data.qnaTraceTraceId,
                self.execution_data.errorMessage,
                total_pages,
                pages_processed,
            ]], self.config.show_query)
        except Exception as ex:
            self.logger.error(f"Error updating file status: {ex}", exc_info=True)

    def _move_file_based_on_status(self, file_path: str, status: FileExecutionStatus) -> None:
        """Mirrors DynamicTemplateLLMParser.moveFileBasedOnStatus()"""
        if not any([self.config.moveCompletedFiles, self.config.moveFailedFiles,
                    self.config.moveUnapprovedFiles]):
            self.logger.info("No target move paths configured — skipping file move.")
            return
        if not os.path.exists(file_path):
            self.logger.info(f"File not found for move: '{file_path}'")
            return

        target_dir = {
            FileExecutionStatus.Completed:  self.config.moveCompletedFiles,
            FileExecutionStatus.Failed:     self.config.moveFailedFiles,
            FileExecutionStatus.Unapproved: self.config.moveUnapprovedFiles,
        }.get(status)

        if not target_dir:
            return

        try:
            os.makedirs(target_dir, exist_ok=True)
        except Exception as ex:
            self.logger.warning(
                f"Cannot create move directory '{target_dir}' — "
                f"skipping file move. Original file remains at '{file_path}'. Error: {ex}"
            )
            return

        try:
            dest = os.path.join(target_dir, os.path.basename(file_path))
            shutil.move(file_path, dest)
            self.logger.info(f"File moved to: {dest}")
        except Exception as ex:
            self.logger.error(
                f"Error moving file '{file_path}' → '{target_dir}': {ex}", exc_info=True
            )

    # ── Orchestration ────────────────────────────────────────────────────────
    def _execute_matched_parser(self, source_path: str) -> None:
        """
        Mirrors Java executeMatchedParser().
        Config and DB are loaded here — source_path is always a single
        file path, guaranteed by _start_processing().
        """
        # ── Load config ───────────────────────────────────────────────────────
        config_error = self._resolve_config(self.config_file_path)
        if config_error:
            self.logger.error(f"Config error: {config_error}")
            self._result_summary["error"] = config_error
            return

        # ── Open DB connection (closed in run()'s finally via _destroy()) ─────
        self._load_db()
        self.logger.info("DB connection established.")

        # ── Process the single file ───────────────────────────────────────────
        self._process_single_file(source_path)

    def _process_single_file(self, file_path: str) -> None:
        """
        Mirrors the updated Java executeMatchedParser() logic.

        1. identify_vendor() returns a TaskResponse (or None).
        2. None response          → Failed  (move file)
        3. isFailed() response    → Failed  (move file)
        4. isFulfilled() response → extract text, continue
        5. Empty extracted text   → Waiting (do NOT move file)
        """
        if not os.path.exists(file_path):
            self.logger.info(f"File not found: '{file_path}'")
            return

        # REPORTING ONLY — create per-file tracking entry
        _file_entry = self._make_file_entry(file_path)
        _file_start_ms = time.time() * 1000

        # ── Step 1: Call Conversation API (identify_vendor) ──────────────────
        task_response = self.identify_vendor(file_path)

        # REPORTING ONLY — capture conversation trace id
        _file_entry["conversation_trace_id"] = self.execution_data.conversationTraceId

        if task_response is None:
            self.logger.info("Conversation API returned None response — marking Failed.")
            self.update_execution_status(file_path, FileExecutionStatus.Failed)
            # REPORTING ONLY
            _file_entry["status"] = str(FileExecutionStatus.Failed)
            _file_entry["error_message"] = "Conversation API returned None response"
            _file_entry["duration_ms"] = round(time.time() * 1000 - _file_start_ms)
            self._result_summary["files"].append(_file_entry)
            return

        self.execution_data.textFromConversationAPI = task_response.value

        if task_response.is_failed():
            self.logger.info("Parser execution stopped because conversation API failed.")
            self.update_execution_status(file_path, FileExecutionStatus.Failed)
            # REPORTING ONLY
            _file_entry["status"] = str(FileExecutionStatus.Failed)
            _file_entry["error_message"] = "Conversation API reported failure"
            _file_entry["duration_ms"] = round(time.time() * 1000 - _file_start_ms)
            self._result_summary["files"].append(_file_entry)
            return

        file_text = ""
        if task_response.is_fulfilled():
            file_text = task_response.value

        # REPORTING ONLY — capture vendor text extracted by the conversation API
        _file_entry["vendor_text"] = file_text

        if not file_text:
            self.logger.info("Conversation API didn't provide matching text — marking Waiting.")
            self.update_execution_status(file_path, FileExecutionStatus.Waiting)
            # REPORTING ONLY
            _file_entry["status"] = str(FileExecutionStatus.Waiting)
            _file_entry["error_message"] = "Conversation API returned no vendor text"
            _file_entry["duration_ms"] = round(time.time() * 1000 - _file_start_ms)
            self._result_summary["files"].append(_file_entry)
            return

        # ── Step 2: Match vendor (match_vendor_parser) ───────────────────────
        match_result = self.match_vendor_parser(file_text)

        # REPORTING ONLY — capture match details
        _file_entry["matched_vendor"]    = match_result.get("vendor_match") and \
                                           match_result["vendor_match"].get(self.config.templateReferenceDBColumn)
        _file_entry["matched_parser_id"] = match_result.get("parser_id")

        if not match_result.get("vendor_match"):
            self.logger.info("No vendor match — marking Unapproved.")
            self.update_execution_status(file_path, FileExecutionStatus.Unapproved)
            # REPORTING ONLY
            _file_entry["status"] = str(FileExecutionStatus.Unapproved)
            _file_entry["error_message"] = "No matching vendor template found"
            _file_entry["duration_ms"] = round(time.time() * 1000 - _file_start_ms)
            self._result_summary["files"].append(_file_entry)
            return

        if not match_result.get("config_class"):
            self.execution_data.errorMessage = "Parser not found in parser_config table."
            self.update_execution_status(file_path, FileExecutionStatus.Failed)
            # REPORTING ONLY
            _file_entry["status"] = str(FileExecutionStatus.Failed)
            _file_entry["error_message"] = self.execution_data.errorMessage
            _file_entry["duration_ms"] = round(time.time() * 1000 - _file_start_ms)
            self._result_summary["files"].append(_file_entry)
            return

        # ── Step 3: Invoke matched parser (_invoke_llm_parser) ───────────────
        result = self._invoke_llm_parser(
            parser_id=match_result["parser_id"],
            config_class=match_result["config_class"],
            file_path=file_path,
        )

        # REPORTING ONLY — capture qna trace id set by the invoked parser
        _file_entry["qna_trace_id"] = self.execution_data.qnaTraceTraceId

        status_map = {0: FileExecutionStatus.Completed, 2: FileExecutionStatus.Waiting}
        final_status = status_map.get(result, FileExecutionStatus.Failed)
        self.update_execution_status(file_path, final_status)

        # REPORTING ONLY — finalise file entry
        _file_entry["status"] = str(final_status)
        # Always surface an error_message on non-Completed outcomes so callers
        # never see status=Failed with error_message=null. Pulled from
        # execution_data (set by the invoked parser on exception), falling back
        # to a generic exit-code description if errorMessage was not populated.
        if final_status != FileExecutionStatus.Completed:
            _file_entry["error_message"] = (
                self.execution_data.errorMessage
                or f"Parser returned exit code {result} (expected 0 for success)"
            )
        _file_entry["duration_ms"] = round(time.time() * 1000 - _file_start_ms)
        self._result_summary["files"].append(_file_entry)

    def _invoke_llm_parser(self, parser_id: str, config_class: str, file_path: str) -> int:
        """_invoke_llm_parser bridge — mirrors Class.forName() reflection in Java."""
        self.logger.info(f"Invoking — class='{config_class}'  parser_id='{parser_id}'")

        JAVA_TO_PYTHON_CLASS_MAP = {
            "com.uxplore.utils.llm.LLMParser": "llm_parser.LLMParser",
        }

        python_class_path = JAVA_TO_PYTHON_CLASS_MAP.get(config_class)

        if not python_class_path:
            error_msg = f"No Python mapping found for Java class: '{config_class}'"
            self.logger.error(error_msg)
            self.execution_data.errorMessage = error_msg
            return 1

        try:
            import importlib
            module_path, class_name = python_class_path.rsplit(".", 1)
            cls      = getattr(importlib.import_module(module_path), class_name)
            instance = cls(parser_id, file_path)

            if hasattr(instance, "set_logger"):
                instance.set_logger(self.logger)

            if hasattr(instance, "set_execution_data"):
                instance.set_execution_data(self.execution_data)

            result = instance.process_file(parser_id, file_path)

            if hasattr(instance, "qna_trace_id"):
                self.execution_data.qnaTraceTraceId = instance.qna_trace_id

            if hasattr(instance, "error_message"):
                self.execution_data.errorMessage = self.execution_data.errorMessage or instance.error_message

            if isinstance(result, dict):
                return result.get("result_code", 1)

            return result

        except Exception as ex:
            self.logger.error(f"Error invoking '{python_class_path}': {ex}", exc_info=True)
            self.execution_data.errorMessage = str(ex)
            return 1

    # ── HTTP helpers ─────────────────────────────────────────────────────────
    def _post_with_file(self, url: str, token: str, file_path: str,
                        form_data: dict, timeout: int) -> str:
        timeout    = max(timeout, 30)
        ext        = os.path.splitext(file_path)[1].lower()
        media_type = {".pdf": "application/pdf", ".png": "image/png",
                      ".jpg": "image/jpeg", ".jpeg": "image/jpeg"}.get(ext, "application/octet-stream")
        self.logger.info(f"POST → {url}  file='{file_path}'")
        with open(file_path, "rb") as fh:
            resp = requests.post(url, headers={"Authorization": f"Bearer {token}"},
                                 files={"files": (os.path.basename(file_path), fh, media_type)},
                                 data=form_data, timeout=timeout)
        if not resp.ok:
            self.logger.error(f"POST failed {resp.status_code}: {resp.text}")
            resp.raise_for_status()
        self.logger.info(f"Response: {resp.text}")
        return resp.text

    def _get_status_by_trace_id(self, trace_id: str) -> str:
        url  = LLMConfig.status_api_url() + trace_id
        self.logger.info(f"GET → {url}")
        resp = requests.get(url, headers={"Authorization": f"Bearer {LLMConfig.token()}"},
                            timeout=max(LLMConfig.http_timeout(), 30))
        if not resp.ok:
            self.logger.error(f"GET failed {resp.status_code}: {resp.text}")
            resp.raise_for_status()
        self.logger.info(f"Response: {resp.text}")
        return resp.text

    # ── Parsing helpers ──────────────────────────────────────────────────────
    @staticmethod
    def _parse_task_detail(json_text: str) -> TaskDetail:
        d = json.loads(json_text)
        return TaskDetail(trace_id=d.get("trace_id", ""), wait_time_ms=d.get("wait_time_ms"))

    @staticmethod
    def _parse_task_response(json_text: str) -> TaskResponse:
        d = json.loads(json_text)
        return TaskResponse(
            status=d.get("status", ""), value=d.get("value", ""),
            requestResponse=d.get("request_response", {}),
            total_pages=int(d.get("total_pages", 0) or 0),
            pages_processed=int(d.get("pages_processed", 0) or 0),
        )

    # ── DB helpers ───────────────────────────────────────────────────────────
    def _get_select_query_results(self, query: str) -> list:
        try:
            self.logger.info(f"Select Query = {query}")
            return self.dao.run_query(query, None)
        except Exception as ex:
            self.logger.error(f"Query failed: {ex}", exc_info=True)
            return []

    def _get_select_query_results_via_parent_account(self, query: str) -> list:
        """Uses .env DB creds — mirrors Parser.getSelectQueryResultsViaParentAccount()"""
        tmp_dao = None
        try:
            tmp_dao = SqlDao(
                db_type   = DBConfig.type(),
                host      = DBConfig.host(),
                port      = DBConfig.port(),
                database  = DBConfig.database_name(),
                user_name = DBConfig.username(),
                password  = DBConfig.password(),
                logger    = self.logger,
            )
            return tmp_dao.run_query(query, None)
        except Exception as ex:
            self.logger.error(f"Parent account query failed: {ex}", exc_info=True)
            return []
        finally:
            if tmp_dao:
                tmp_dao.close()

    def _destroy(self) -> None:
        if self.dao:
            self.dao.close()
            self.dao = None
        close_instance_logger(self.logger)

    @staticmethod
    def _get_parser_execution_id() -> str:
        """Mirrors DynamicTemplateLLMParser.getParserExecutionId()"""
        pid = os.environ.get("LLM_PARSER_PARSER_EXECUTION_ID")
        if not pid:
            import random
            pid = str(1000 + random.randint(0, 31768)) + str(int(time.time() * 1000) % 100000)
            os.environ["LLM_PARSER_PARSER_EXECUTION_ID"] = pid
        return pid
    
    @staticmethod
    def _start_processing(config: str, source: str) -> list:
        """
        Single authoritative entry point for directory fan-out and thread pool.
        Called by both __main__ (CLI) and handle_request (main file).
        Mirrors Java DynamicTemplateLLMParser.startProcessing().

        Always creates one instance per file — directory expansion never
        happens inside a parser instance (see _execute_matched_parser).

        Returns list of _result_summary dicts, one per file processed.
        """
        files = (
            DynamicTemplateLLMParser._get_files_by_filter(source)
            if os.path.isdir(source)
            else [os.path.abspath(source)]
        )

        if not files:
            static_logger.info(f"No supported files found in: '{source}'")
            return []

        summaries = []

        if len(files) == 1:
            summaries = [DynamicTemplateLLMParser(config, files[0]).run()]
        else:
            with concurrent.futures.ThreadPoolExecutor(
                max_workers=os.cpu_count() + 2
            ) as executor:
                future_map = {
                    executor.submit(DynamicTemplateLLMParser(config, f).run): f
                    for f in files
                }
                for future in concurrent.futures.as_completed(future_map):
                    try:
                        summaries.append(future.result())
                    except Exception as ex:
                        static_logger.error(f"File processing error: {ex}", exc_info=True)

        # ── CLI summary log ───────────────────────────────────────────────────
        all_files  = [fe for s in summaries for fe in s.get("files", [])]
        completed  = sum(1 for f in all_files if f.get("status") == "Completed")
        failed     = sum(1 for f in all_files if f.get("status") == "Failed")
        waiting    = sum(1 for f in all_files if f.get("status") == "Waiting")
        unapproved = sum(1 for f in all_files if f.get("status") == "Unapproved")
        skipped    = sum(s.get("skipped_files", 0) for s in summaries)

        static_logger.info("─── Execution Summary ───────────────────────────────")
        static_logger.info(f"  Source     : {source}")
        static_logger.info(f"  Total      : {len(all_files)}")
        static_logger.info(f"  Completed  : {completed}")
        static_logger.info(f"  Failed     : {failed}")
        static_logger.info(f"  Waiting    : {waiting}")
        static_logger.info(f"  Unapproved : {unapproved}")
        static_logger.info(f"  Skipped    : {skipped}")
        static_logger.info("─────────────────────────────────────────────────────")
        for f in all_files:
            status = f.get("status", "unknown")
            name   = f.get("file_name", "unknown")
            err    = f.get("error_message", "")
            line   = f"  [{status}] {name}"
            if err:
                line += f" — {err}"
            static_logger.info(line)

        return summaries


# ===========================================================================
# CLI entry point
# ===========================================================================
if __name__ == "__main__":
    import argparse
    
    ap = argparse.ArgumentParser(description="DynamicTemplateLLMParser")
    ap.add_argument("--config", required=True, help="Config JSON file path or parser id")
    ap.add_argument("--source", required=True, help="File or directory path for parsing")
    args = ap.parse_args()

    static_logger.info("DynamicTemplateLLMParser execution started")
    DynamicTemplateLLMParser._start_processing(args.config, args.source)
    static_logger.info("DynamicTemplateLLMParser execution end")