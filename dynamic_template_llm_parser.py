"""
Python equivalent of DynamicTemplateLLMParser.java

Tools implemented:
    TOOL 1 - validate_input          : load config (from LLMConfigBean or DB using id),
                                       check file/dir, extension, multi-invoice skip
    TOOL 2 - identify_vendor         : send doc to Conversation API, extract vendor text
    TOOL 3 - match_vendor_parser     : match vendor text against DB, get parser class
    TOOL 5 - update_execution_status : write status to DB, move files, cleanup

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
from config import cfg, cfg_int, cfg_bool, DBConfig, LLMConfig, MultiInvoiceConfig
from constants import MYSQL_DB, FileExecutionStatus, TaskExecutionStatus
from logger import get_static_logger, get_instance_logger, close_instance_logger
from sql_dao import SqlDao
from models.llm_config_bean import LLMConfigBean
from models.execution_data import ExecutionData
from models.task_detail import TaskDetail
from models.task_response import TaskResponse



static_logger = get_static_logger("DynamicTemplateLLMParser")
SUPPORTED_EXT_RE = re.compile(r"\.(pdf|png|jpg|jpeg)$", re.IGNORECASE)


# class MultiInvoiceChecker:
#     def __init__(self) -> None:
#         self._threshold = MultiInvoiceConfig.page_threshold()
#         static_logger.info(f"MultiInvoiceChecker — page threshold = {self._threshold}")

#     def should_skip_file(self, file_path: str) -> bool:
#         ext = os.path.splitext(file_path)[1].lower()
#         if ext in (".png", ".jpg", ".jpeg"):
#             return False
#         if ext == ".pdf":
#             return self._pdf_exceeds_threshold(file_path)
#         return False

#     def _pdf_exceeds_threshold(self, file_path: str) -> bool:
#         try:
#             from pypdf import PdfReader
#         except ImportError:
#             try:
#                 from PyPDF2 import PdfReader
#             except ImportError:
#                 static_logger.warning("pypdf not installed — defaulting to NOT skip.")
#                 return False
#         try:
#             reader = PdfReader(file_path)
#             return len(reader.pages) > self._threshold
#         except Exception as exc:
#             static_logger.warning(f"Could not read PDF: {exc} — defaulting to NOT skip.")
#             return False


class DynamicTemplateLLMParser:
    """
    Python equivalent of DynamicTemplateLLMParser.java

    Usage:
        parser = DynamicTemplateLLMParser(config_file_path, source_input)
        parser.run()

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

    # ── run ─────────────────────────────────────────────────────────────────
    def run(self) -> None:
        try:
            self._execute_matched_parser(self.source_input)
        except Exception as ex:
            self.logger.error(f"Fatal error: {ex}", exc_info=True)
        finally:
            if self.dao:
                self._destroy()

    # ── TOOL 1 — validate_input ─────────────────────────────────────────────
    def tool1_validate_input(self, config_input: str, source_path: str) -> dict:
        """
        TOOL 1: validate_input
        Step A — Config resolution:
            - If `config_input` is an LLMConfigBean instance, use it directly.
            - If `config_input` is a path to a JSON file, load it from disk.
            - If `config_input` is a numeric string (parser id), fetch config from DB.
            Sets self.config and opens self.dao on success.

        Step B — Source path validation:
            - Checks the file/directory exists.
            - Filters files by supported extensions (.pdf, .png, .jpg, .jpeg).
            - Runs multi-invoice skip check (if enabled via env).

        Mirrors:
            Tool 0: DynamicTemplateLLMParser.loadConfig()
            Tool 1: DynamicTemplateLLMParser.startProcessing()

        Returns:
            {
                "config_loaded": bool,
                "source_type":   "file" | "directory" | "unknown",
                "valid_files":   [ {"path": str, "valid": bool, "skip": bool, "reason": str} ],
                "skipped_files": [...],
                "error":         str | None
            }
        """
        self.logger.info(f"TOOL 1 — validate_input: config='{config_input}'  source='{source_path}'")

        # ── Step A: Resolve & load config ────────────────────────────────────
        config_error = self._resolve_config(config_input)
        if config_error:
            return {
                "config_loaded": False,
                "source_type": "unknown",
                "valid_files": [],
                "skipped_files": [],
                "error": config_error,
            }

        # # Open the DAO connection using the freshly loaded config
        self._load_db()
        self.logger.info("DB connection established.")

        # ── Step B: Validate source path ─────────────────────────────────────
        if not os.path.exists(source_path):
            msg = f"Source path does not exist: '{source_path}'"
            self.logger.error(msg)
            return {
                "config_loaded": True,
                "source_type": "unknown",
                "valid_files": [],
                "skipped_files": [],
                "error": msg,
            }

        is_directory    = os.path.isdir(source_path)
        source_type     = "directory" if is_directory else "file"
        candidate_paths = (
            DynamicTemplateLLMParser._get_files_by_filter(source_path)
            if is_directory else [os.path.abspath(source_path)]
        )

        if is_directory and not candidate_paths:
            self.logger.info(f"No supported files found in directory: '{source_path}'")

        # checker = MultiInvoiceChecker() if MultiInvoiceConfig.skip() else None
        checker      = None
        valid_files  = []
        skipped_files = []

        for abs_path in candidate_paths:
            if not SUPPORTED_EXT_RE.search(abs_path):
                skipped_files.append({
                    "path": abs_path, "valid": False, "skip": False,
                    "reason": f"Unsupported extension: '{os.path.splitext(abs_path)[1]}'"
                })
                continue
            if checker and checker.should_skip_file(abs_path):
                self.logger.info(f"Skipping multi-page invoice: {abs_path}")
                skipped_files.append({
                    "path": abs_path, "valid": True, "skip": True,
                    "reason": "Multi-invoice skip (HACHIAI_LLM_MULTI_INVOICE_SKIP=true)"
                })
                continue
            valid_files.append({"path": abs_path, "valid": True, "skip": False, "reason": ""})

        self.logger.info(
            f"TOOL 1 complete — config_loaded=True  valid={len(valid_files)}  skipped={len(skipped_files)}"
        )
        return {
            "config_loaded": True,
            "source_type": source_type,
            "valid_files": valid_files,
            "skipped_files": skipped_files,
            "error": None,
        }

    def _resolve_config(self, config_input) -> Optional[str]:
        """
        Resolves config from three possible inputs and sets self.config.
        Returns an error string on failure, or None on success.
        Mirrors the original Tool 0 / loadConfig() logic.
        """
        # Already a loaded LLMConfigBean — use directly
        if isinstance(config_input, LLMConfigBean):
            self.logger.info("Config provided as LLMConfigBean — using directly.")
            self.config = config_input
            return None

        config_input = str(config_input).strip()

        # Path to a JSON config file on disk
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

        # Numeric string — treat as parser id, fetch from DB
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
        """Short-lived connection using .env DB creds. Mirrors Parser.getParserConfigFromDB()"""
        tmp_dao = None
        try:
            tmp_dao = SqlDao(
                db_type = DBConfig.type(),
                host  = DBConfig.host(),
                port = DBConfig.port(),
                database = DBConfig.database_name(),
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
            db_type = DBConfig.type(),
            host  = DBConfig.host(),
            port = DBConfig.port(),
            database = DBConfig.database_name(),
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

    # ── TOOL 2 — identify_vendor ────────────────────────────────────────────
    def tool2_identify_vendor(self, file_path: str) -> Optional[TaskResponse]:
        """
        TOOL 2: identify_vendor  (1st LLM API call)
        Sends document to Conversation API, polls for result, returns full TaskResponse.
 
        CHANGED (matches updated Java): now returns the full TaskResponse object
        instead of just the extracted string value. Callers are responsible for
        checking task_response.is_fulfilled() / task_response.is_failed().
 
        Mirrors: DynamicTemplateLLMParser.getInputFileText() [updated Java version]
        """
        self.logger.info(f"TOOL 2 — identify_vendor: file='{file_path}'")
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

    # ── TOOL 3 — match_vendor_parser ────────────────────────────────────────
    def tool3_match_vendor_parser(self, vendor_text: str) -> dict:
        """
        TOOL 3: match_vendor_parser
        Queries vendor reference table for a match, then fetches parser config.
        Mirrors: DynamicTemplateLLMParser.getVendorParserDetailIfMatchedInFile()
        Returns:
            {
                "vendor_match": dict | None,
                "parser_id":    str | None,
                "config_class": str | None,
                "parser_type":  str | None,
            }
        """
        self.logger.info(f"TOOL 3 — match_vendor_parser: vendor_text='{vendor_text}'")
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
            "FROM parser_config pc "
            "LEFT JOIN parser_config_type pct ON pc.parser_type = pct.parser_type "
            f"WHERE pc.id = {parser_id};"
        )
        config_rows  = self._get_select_query_results_via_parent_account(query)
        if not config_rows:
            self.logger.info(f"parser_id={parser_id} not found in parser_config.")
            return {"vendor_match": matched_vendor, "parser_id": parser_id,
                    "config_class": None, "parser_type": None}

        config_class = config_rows[0].get("config_class", "")
        parser_type  = config_rows[0].get("parser_type", "")
        self.logger.info(f"Matched — parser_id={parser_id}  config_class={config_class}  parser_type={parser_type}")
        return {"vendor_match": matched_vendor, "parser_id": parser_id,
                "config_class": config_class, "parser_type": parser_type}

    # ── TOOL 5 — update_execution_status ────────────────────────────────────
    def tool5_update_execution_status(self, file_path: str, status: FileExecutionStatus) -> None:
        """
        TOOL 5: update_execution_status  (always runs — mirrors Java finally block)
        1. Writes status row to DB.
        2. Moves file to configured directory.
        3. Closes DB + flushes log handlers.
        """
        self.logger.info(f"TOOL 5 — update_execution_status: status={status}  file='{file_path}'")
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

        # total_pages and pages_processed come from executionData.taskResponse
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
        os.makedirs(target_dir, exist_ok=True)
        try:
            shutil.move(file_path, os.path.join(target_dir, os.path.basename(file_path)))
            self.logger.info(f"File moved to: {target_dir}")
        except Exception as ex:
            self.logger.error(f"Error moving file: {ex}", exc_info=True)

    # ── Orchestration ────────────────────────────────────────────────────────
    def _execute_matched_parser(self, source_path: str) -> None:
        validation = self.tool1_validate_input(self.config_file_path, source_path)
        if validation["error"]:
            self.logger.info(f"Validation error: {validation['error']}")
            return
        for file_entry in validation["valid_files"]:
            self._process_single_file(file_entry["path"])

    
    def _process_single_file(self, file_path: str) -> None:
        """
        CHANGED: Mirrors the updated Java executeMatchedParser() logic.
 
        Old behaviour: tool2_identify_vendor() returned a string; empty string → Waiting.
        New behaviour:
          1. tool2_identify_vendor() returns a TaskResponse (or None).
          2. None response          → Failed  (move file)
          3. isFailed() response    → Failed  (move file)
          4. isFulfilled() response → extract text, continue
          5. Empty extracted text   → Waiting (do NOT move file)
        """
        if not os.path.exists(file_path):
            self.logger.info(f"File not found: '{file_path}'")
            return
 
        # ── Step 1: Call Conversation API (Tool 2) ───────────────────────────
        task_response = self.tool2_identify_vendor(file_path)
 
        if task_response is None:
            # Response object itself is None — unexpected failure
            self.logger.info("Conversation API returned None response — marking Failed.")
            self.tool5_update_execution_status(file_path, FileExecutionStatus.Failed)
            return
 
        # Store raw response value in execution data regardless of status
        self.execution_data.textFromConversationAPI = task_response.value
 
        if task_response.is_failed():
            # API explicitly reported failure
            self.logger.info("Parser execution stopped because conversation API failed.")
            self.tool5_update_execution_status(file_path, FileExecutionStatus.Failed)
            return
 
        # Extract text only when fulfilled
        file_text = ""
        if task_response.is_fulfilled():
            file_text = task_response.value
 
        if not file_text:
            # Fulfilled but empty, or neither fulfilled nor failed (e.g. still pending)
            self.logger.info("Conversation API didn't provide matching text — marking Waiting.")
            self.tool5_update_execution_status(file_path, FileExecutionStatus.Waiting)
            return
 
        # ── Step 2: Match vendor (Tool 3) ────────────────────────────────────
        match_result = self.tool3_match_vendor_parser(file_text)
 
        if not match_result.get("vendor_match"):
            self.logger.info("No vendor match — marking Unapproved.")
            self.tool5_update_execution_status(file_path, FileExecutionStatus.Unapproved)
            return
 
        if not match_result.get("config_class"):
            self.execution_data.errorMessage = "Parser not found in parser_config table."
            self.tool5_update_execution_status(file_path, FileExecutionStatus.Failed)
            return
 
        # ── Step 3: Invoke matched parser (Tool 4) ───────────────────────────
        result = self._invoke_llm_parser(
            parser_id=match_result["parser_id"],
            config_class=match_result["config_class"],
            file_path=file_path,
        )
        status_map = {0: FileExecutionStatus.Completed, 2: FileExecutionStatus.Waiting}
        self.tool5_update_execution_status(file_path, status_map.get(result, FileExecutionStatus.Failed))

    def _invoke_llm_parser(self, parser_id: str, config_class: str, file_path: str) -> int:
        """TOOL 4 bridge — mirrors Class.forName() reflection in Java."""
        self.logger.info(f"Invoking — class='{config_class}'  parser_id='{parser_id}'")
        
        # ── Java → Python class mapping ──────────────────────────────────────
        JAVA_TO_PYTHON_CLASS_MAP = {
            "com.uxplore.utils.llm.LLMParser": "llm_parser.LLMParser",
            # Add more mappings here as you port other parsers:
            # "com.uxplore.utils.llm.SomeOtherParser":        "some_other_parser.SomeOtherParser",
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
                db_type = DBConfig.type(),
                host  = DBConfig.host(),
                port = DBConfig.port(),
                database = DBConfig.database_name(),
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
        pid = os.environ.get("PARSER_EXECUTION_ID")
        if not pid:
            import random
            pid = str(1000 + random.randint(0, 31768)) + str(int(time.time() * 1000) % 100000)
            os.environ["PARSER_EXECUTION_ID"] = pid
        return pid


# ===========================================================================
# CLI entry point
# ===========================================================================
if __name__ == "__main__":
    import argparse
    import concurrent.futures

    ap = argparse.ArgumentParser(description="DynamicTemplateLLMParser")
    ap.add_argument("--config", required=True, help="Config JSON file path or parser id")
    ap.add_argument("--source", required=True, help="File or directory path for parsing")
    args = ap.parse_args()

    static_logger.info("DynamicTemplateLLMParser execution started")
    source  = args.source
    is_dir  = os.path.isdir(source)
    # checker = MultiInvoiceChecker() if MultiInvoiceConfig.skip() else None
    checker = None

    if is_dir:
        files = DynamicTemplateLLMParser._get_files_by_filter(source)
        if not files:
            static_logger.info(f"No files found in directory: {source}")
        with concurrent.futures.ThreadPoolExecutor(max_workers=os.cpu_count() + 2) as executor:
            for f in files:
                if checker and checker.should_skip_file(f):
                    static_logger.info(f"Skipping multi-page invoice: {f}")
                    continue
                executor.submit(DynamicTemplateLLMParser(args.config, f).run)
    else:
        if checker and checker.should_skip_file(source):
            static_logger.info(f"Skipping multi-page invoice: {source}")
            exit(0)
        DynamicTemplateLLMParser(args.config, source).run()

    static_logger.info("DynamicTemplateLLMParser execution end")