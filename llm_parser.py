"""
Python equivalent of LLMParser.java

TOOL 4 — execute_parser

Responsibility:
  - Load prompts from DB (llmPromptDatabaseTable)
  - Build query payload JSON
  - Send document + prompts to QnA API (async POST)
  - Poll status API until fulfilled / failed / waiting
  - Parse JSON response
  - Insert extracted records into DB

Called by DynamicTemplateLLMParser._invoke_llm_parser() after vendor match.
Can also run standalone via CLI:
    python llm_parser.py --config <id> --source <file_or_dir>
"""

import json
import os
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional
import requests

from constants import FileExecutionStatus, TaskExecutionStatus, DATA_IGNORE

from logger import get_static_logger, get_instance_logger, close_instance_logger
from sql_dao import SqlDao
from config import DBConfig, LLMConfig, cfg

from models.llm_config_bean import LLMConfigBean
from models.execution_data import ExecutionData
from models.task_detail import TaskDetail
from models.task_response import TaskResponse
from models.db_column_mapping_bean import DBColumnMappingBean
from models.config_file_bean import ConfigFileBean
from constants import MSSQL_DB, MYSQL_DB

# Static logger
static_logger = get_static_logger("LLMParser")


# DBColumnMappingBean  (mirrors com.uxplore.utils.llm.bean.DBColumnMappingBean)
# ConfigFileBean  (mirrors com.uxplore.utils.common.bean.ConfigFileBean)


# LLMParser
class LLMParser:
    """
    Python equivalent of LLMParser.java

    Usage (standalone):
        parser = LLMParser(config_path, source_input)
        result = parser.process_file(config_path, source_input)

    Usage (called from DynamicTemplateLLMParser):
        instance = LLMParser(parser_id, file_path)
        instance.set_logger(logger)
        instance.set_execution_data(execution_data)
        result = instance.process_file(parser_id, file_path)
    """

    def __init__(self, config_path: str, source_input: str) -> None:
        self.config_path   = config_path
        self.source_input  = source_input
        self.parser_id:    Optional[str] = None
        self.parser_name:  str = ""
        self.service_type: str = "vision_quest;minicmp2_6"

        self.config:           Optional[LLMConfigBean] = None
        self.dao:              Optional[SqlDao]         = None
        self.column_format_list: List[DBColumnMappingBean] = []
        self.column_list:      str = ""
        self.file_bean:        Optional[ConfigFileBean] = None
        self.execution_data:   Optional[ExecutionData]  = None

        self.qna_trace_id:  str = ""
        self.error_message: str = ""

        # Logger — overridden by set_logger() when called from DynamicTemplateLLMParser
        self._own_execution_id = str(uuid.uuid4())
        self.logger = get_instance_logger(self._own_execution_id, "LLMParser")

        self.query_table_column_map: List[Dict[str, str]] = []

    # ── Setters called by DynamicTemplateLLMParser ──────────────────────────
    def set_logger(self, logger) -> None:
        """Share the parent parser's logger instead of creating a new one."""
        self.logger = logger

    def set_execution_data(self, execution_data: ExecutionData) -> None:
        self.execution_data = execution_data

    # ── Public entry point ───────────────────────────────────────────────────
    def run(self) -> None:
        """Mirrors Runnable.run()"""
        try:
            self.process_file(self.config_path, self.source_input)
        except Exception as ex:
            self.logger.error(f"Fatal error: {ex}", exc_info=True)

    def process_file(self, config_path: str, pdf_path: str) -> int:
        """
        TOOL 4 main method.

        Mirrors LLMParser.processFile()
        Returns: 0=success, 1=failed, 2=waiting
        """
        self.logger.info(f"process_file: '{pdf_path}'")
        self._load_config(config_path)
        self._load_db()
        self._load_column_config()
        rtn = self._load_file(pdf_path)
        self._destroy()
        if rtn == 0:
            self.logger.info("Data import completed successfully.")
        else:
            self.logger.info("Data import failed.")
        return rtn

    # Config / DB loading
    def _load_config(self, config_input: str) -> None:
        """Mirrors LLMParser.loadConfig()"""
        if os.path.isfile(config_input):
            self.logger.info(f"Using config file: {config_input}")
            with open(config_input, "r", encoding="utf-8") as f:
                d = json.load(f)
            self.config    = LLMConfigBean.from_dict(d)
            self.parser_id = self.config.id
        elif config_input.strip().isdigit():
            self.logger.info(f"Using DB config for parser id: {config_input}")
            rows = self._get_parser_config_from_db(config_input)
            if not rows:
                raise RuntimeError(f"No active config found for parser id: {config_input}")
            config_data = json.loads(rows[0]["config"])
            self.config       = LLMConfigBean(**config_data)
            self.parser_id    = config_input
            self.parser_name  = rows[0].get("name", "")
            self.service_type = rows[0].get("service_type") or "vision_quest;minicmp2_6"
        else:
            raise RuntimeError(f"Config not found: {config_input}")

        if self.config.show_query:
            self.logger.info(f"Config: {self.config}")

        # Sort commonFields by id  (mirrors Java Comparator)
        self.column_format_list = []

    def _get_parser_config_from_db(self, config_input: str) -> list:
        tmp = None
        try:
            tmp = SqlDao(
                db_type = DBConfig.type(),
                host  = DBConfig.host(),
                port = DBConfig.port(),
                database = DBConfig.database_name(),
                user_name = DBConfig.username(),
                password  = DBConfig.password(),
                logger    = self.logger,
            )
            return tmp.run_query(
                "SELECT * FROM parser_config WHERE status = %s AND (id = %s OR name = %s)",
                ["active", config_input, config_input],
            )
        except Exception as ex:
            self.logger.error(f"DB config fetch failed: {ex}", exc_info=True)
            return []
        finally:
            if tmp:
                tmp.close()

    def _load_db(self) -> None:
        """Mirrors LLMParser.loadDB()"""
        self.dao = SqlDao(
            db_type = DBConfig.type(),
            host  = DBConfig.host(),
            port = DBConfig.port(),
            database = DBConfig.database_name(),
            user_name = DBConfig.username(),
            password  = DBConfig.password(),
            logger    = self.logger,
        )

    def _load_column_config(self) -> None:
        """
        Mirrors LLMParser.loadColumnConfig()
        Reads prompt rows from llmPromptDatabaseTable and builds column_format_list.
        """
        self.query_table_column_map = []
        common_db_columns: List[str] = []

        # Check for prompt_order column
        check_q = (
            f"SELECT COUNT(*) as column_count FROM information_schema.columns "
            f"WHERE table_schema = DATABASE() AND table_name = '{self.config.llmPromptDatabaseTable}' "
            f"AND column_name = 'prompt_order'"
        )
        check_rows = self._get_select_query_results(check_q)
        has_prompt_order = int((check_rows[0].get("column_count", 0) or 0)) > 0 if check_rows else False

        query = (
            f"SELECT * FROM {self.config.llmPromptDatabaseTable} "
            f"WHERE parser_id = {self.parser_id}"
            + (" ORDER BY prompt_order, id" if has_prompt_order else "")
        )
        prompt_rows = self._get_select_query_results(query)
        if not prompt_rows:
            self.logger.info(f"No prompts found for parser_id={self.parser_id}")
            return

        # Common columns
        for row in prompt_rows:
            col = row.get("db_column", "")
            if (row.get("column_type", "").lower() == "common"
                    and col and col.lower() != "metadata"):
                common_db_columns.append(col)

        for i, col in enumerate(common_db_columns):
            bean = DBColumnMappingBean(id=i + 1, data_type="varchar", sql_column_name=col)
            self.column_format_list.append(bean)

        # parser_file_execution_id column
        if self.config.parserFileExecutionIdColumn:
            bean = DBColumnMappingBean(
                id              = len(self.column_format_list) + 1,
                data_type       = "string",
                sql_column_name = self.config.parserFileExecutionIdColumn,
            )
            self.column_format_list.append(bean)

        # Table columns
        for row in prompt_rows:
            if row.get("column_type", "").lower() == "table" and row.get("db_column"):
                result_map = self._map_columns_to_values(
                    row["prompt"], row["db_column"], remove_first=True
                )
                if result_map:
                    for key in result_map:
                        bean = DBColumnMappingBean(
                            id              = len(self.column_format_list) + 1,
                            data_type       = "varchar",
                            sql_column_name = key,
                        )
                        self.column_format_list.append(bean)
                    self.query_table_column_map.append(result_map)

        # file_name column
        if self.config.fileNameColumn:
            bean = DBColumnMappingBean(
                id              = len(self.column_format_list) + 1,
                data_type       = "variable",
                format          = ["$FILE_NAME"],
                sql_column_name = self.config.fileNameColumn,
            )
            self.column_format_list.append(bean)

        self.column_list = self._get_columns_list(self.column_format_list)

    # -----------------------------------------------------------------------
    # File processing
    # -----------------------------------------------------------------------
    def _load_file(self, path: str) -> int:
        """Mirrors LLMParser.loadFile()"""
        if not os.path.exists(path):
            self.logger.info(f"Source file does not exist: '{path}'")
            return 1
        try:
            return self._process_input_file(path)
        except Exception as ex:
            self.logger.error(f"Fatal: {ex}", exc_info=True)
            return 1

    def _process_input_file(self, file_path: str) -> int:
        """
        Mirrors LLMParser.processInputFile()
        Full pipeline: send to QnA API → poll → parse → insert to DB.
        """
        return_value = 0
        try:
            st = time.time()
            self.file_bean = ConfigFileBean(
                fileName  = os.path.basename(file_path),
                startDate = datetime.now(),
                status    = "CREATED",
            )

            # Send to LLM QnA API
            task_detail   = self._send_llm_request_and_get_task_detail(file_path)
            self.qna_trace_id = task_detail.trace_id

            task_response = self._track_execution_status(task_detail)
            
            if self.execution_data:
                self.execution_data.taskResponse = task_response

            if task_response.is_fulfilled():
                result      = self._parse_response(task_response.requestResponse)
                records     = self._process_response_data(result)
                self.logger.info(f"Total records found: {len(records)}")
                return_value = self._load_records_to_db(records)
                elapsed = int((time.time() - st) * 1000)
                self.logger.info(f"Completed {len(records)} records in {elapsed}ms")

            elif task_response.status.lower() == FileExecutionStatus.Waiting.value.lower():
                self.logger.error(f"Task trace_id={task_detail.trace_id} status=Waiting")
                return_value = 2
            
            else:
                self.logger.error(f"Task trace_id={task_detail.trace_id} status=Failed")
                return_value = 1

        except Exception as ex:
            self.logger.error(f"processInputFile error: {ex}", exc_info=True)
            return_value = 1
            self.error_message = self.error_message or str(ex)

        return return_value

    # -----------------------------------------------------------------------
    # Prompt builder
    # -----------------------------------------------------------------------
    def _get_parser_prompt_as_llm_query(self) -> Optional[str]:
        """
        Mirrors LLMParser.getParserPromptAsLLMQuery()
        Builds the {"Queries": [...]} JSON string from DB prompt rows.
        """
        check_q = (
            f"SELECT COUNT(*) as column_count FROM information_schema.columns "
            f"WHERE table_schema = DATABASE() AND table_name = '{self.config.llmPromptDatabaseTable}' "
            f"AND column_name = 'prompt_order'"
        )
        check_rows      = self._get_select_query_results(check_q)
        has_prompt_order = int((check_rows[0].get("column_count", 0) or 0)) > 0 if check_rows else False

        query = (
            f"SELECT * FROM {self.config.llmPromptDatabaseTable} "
            f"WHERE parser_id = {self.parser_id}"
            + (" ORDER BY prompt_order, id" if has_prompt_order else "")
        )
        prompt_rows = self._get_select_query_results(query)
        if not prompt_rows:
            self.logger.info(f"No prompts found for parser_id={self.parser_id}")
            return None

        queries = []
        for row in prompt_rows:
            prompt          = row.get("prompt", "")
            alias           = row.get("db_column", "")
            value_type      = row.get("value_type") or "string"
            is_required     = row.get("mandatory_value")
            key             = row.get("name", "")
            confidence_score = row.get("confidence_score", "")
            column_type     = (row.get("column_type") or "").lower()

            if column_type == "table":
                table_rows = self._map_columns_to_values_full(
                    prompt, alias, value_type, is_required, key, confidence_score,
                    remove_first=False
                )
                main_row    = table_rows[0]
                sub_rows    = table_rows[1:]
                table_entry = {
                    "Table": {
                        "query": self._build_query_entry(main_row),
                        "queries": [self._build_query_entry(r) for r in sub_rows],
                    }
                }
                queries.append(table_entry)
            else:
                entry = {
                    "Text":       prompt,
                    "Alias":      alias,
                    "IsRequired": self._default_mandatory(is_required),
                    "Type":       value_type or "string",
                }
                if key:
                    entry["Key"] = key
                if confidence_score:
                    entry["Confidence_Threshold"] = float(confidence_score)
                queries.append(entry)

        return json.dumps({"Queries": queries})

    @staticmethod
    def _build_query_entry(row: dict) -> dict:
        entry = {
            "Text":       row.get("Text", ""),
            "Alias":      row.get("Alias", ""),
            "IsRequired": LLMParser._default_mandatory(row.get("IsRequired")),
            "Type":       row.get("Type") or "string",
        }
        if row.get("Key"):
            entry["Key"] = row["Key"]
        if row.get("confidence_score"):
            entry["Confidence_Threshold"] = float(row["confidence_score"])
        return entry

    @staticmethod
    def _default_mandatory(value: Any) -> bool:
        if value is None or str(value).strip() == "":
            return True
        return str(value).lower() == "true"

    # -----------------------------------------------------------------------
    # LLM API calls
    # -----------------------------------------------------------------------
    def _send_llm_request_and_get_task_detail(self, file_path: str) -> TaskDetail:
        """Mirrors LLMParser.sendLLMRequestAndGetTaskDetail()"""
        query_payload     = self._get_parser_prompt_as_llm_query()
        api_url           = LLMConfig.api_url()
        api_token         = LLMConfig.token()
        timeout           = LLMConfig.http_timeout()
        max_token         = LLMConfig.max_token()
        temperature       = LLMConfig.temperature()
        enable_validation = LLMConfig.enable_validation()
        enable_confidence = LLMConfig.enable_confidence()

        exec_id  = self.execution_data.parserFileExecutionId if self.execution_data else str(uuid.uuid4())
        tags_json = json.dumps([f"execution_id={exec_id}", "qna"])

        form_data = {
            "query":                  query_payload,
            "max_token":              max_token,
            "temperature":            temperature,
            "enable_validation":      enable_validation,
            "enable_confidence_score": enable_confidence,
            "tags":                   tags_json,
            "service_type":           self.service_type,
        }

        response_text = self._post_with_file(api_url, api_token, file_path, form_data, timeout)
        return self._parse_task_detail(response_text)

    
    def _track_execution_status(self, task_detail: TaskDetail) -> TaskResponse:
        """Mirrors LLMParser.trackExecutionStatus()"""
        total_iterations = LLMConfig.status_total_iterations()
        interval_ms      = LLMConfig.status_interval_minutes() * 60 * 1000

        if task_detail.wait_time_ms:
            interval_ms = int(task_detail.wait_time_ms)

        trace_id = task_detail.trace_id
        if not trace_id:
            raise ValueError("Trace ID missing in QnA API task response.")

        task_response = TaskResponse()
        for attempt in range(1, total_iterations + 1):
            self.logger.info(f"TraceId:{trace_id} — Attempt #{attempt}: waiting {interval_ms}ms...")
            time.sleep(interval_ms / 1000)

            response_text = self._get_status_by_trace_id(trace_id)
            task_response = self._parse_task_response(response_text)

            if self.execution_data:
                self.execution_data.taskResponse = task_response

            if task_response.status.lower() == TaskExecutionStatus.fulfilled.value:
                self.logger.info(f"TraceId:{trace_id} — Task completed.")
                return task_response
            
            elif task_response.status.lower() == TaskExecutionStatus.failed.value:
                self.logger.info(f"TraceId:{trace_id} — Task failed.")
                self.error_message = self.error_message or f"Task failed: trace_id={trace_id}"
                return task_response

        self.logger.info(f"TraceId:{trace_id} — Did not complete in allowed iterations — marking Waiting.")
        task_response.status = FileExecutionStatus.Waiting.value
        return task_response

    # -----------------------------------------------------------------------
    # Response parsing
    # -----------------------------------------------------------------------
    def _parse_response(self, response: dict) -> Dict[str, Any]:
        """
        Mirrors LLMParser.parseResponse()
        Converts the API response dict into a flat {alias: value} map,
        where table data becomes {alias: List[Dict[str, str]]}.
        """
        result: Dict[str, Any] = {}
        response_array = (response.get("result") or {}).get("response", [])

        for node in response_array:
            alias      = node.get("alias", "")
            text_val   = node.get("text", "")
            table_val  = node.get("table")

            if isinstance(table_val, list):
                table_list = []
                for row_node in table_val:
                    row_map = {}
                    for cell in row_node:
                        row_map[cell.get("alias", "")] = cell.get("text", "")
                    table_list.append(row_map)
                result[alias] = table_list
            else:
                result[alias] = text_val

        return result

    def _process_response_data(self, result: Dict[str, Any]) -> List[List[str]]:
        """
        Mirrors LLMParser.processResponseData()
        Builds list of string arrays (one per DB row) from parsed response.
        """
        records: List[List[str]] = []
        exec_id_index = 0

        common_record = [""] * len(self.column_format_list)
        for i, bean in enumerate(self.column_format_list):
            col = bean.sql_column_name
            if col in result and isinstance(result[col], str):
                common_record[i] = result[col]
                exec_id_index   += 1

        # Inject parserFileExecutionId
        if self.config.parserFileExecutionIdColumn and self.execution_data:
            common_record[exec_id_index] = self.execution_data.parserFileExecutionId

        # Collect table data
        tables = {k: v for k, v in result.items() if isinstance(v, list)}

        if not tables:
            records.append(common_record)
            return records

        for table_key, table_rows in tables.items():
            for row in table_rows:
                record = common_record.copy()
                for i, bean in enumerate(self.column_format_list):
                    col = bean.sql_column_name
                    if col and col in row:
                        record[i] = row[col]
                records.append(record)

        return records

    # -----------------------------------------------------------------------
    # DB insert
    # -----------------------------------------------------------------------
    def _load_records_to_db(self, records: List[List[str]]) -> int:
        """Mirrors LLMParser.loadRecordsToDB()"""
        if not records:
            self.logger.error("No records to insert.")
            return 0

        db_type = self.config.sql
        if db_type.lower() == MYSQL_DB:
            query = f"INSERT IGNORE INTO {self.config.table} ({self.column_list}) VALUES \n"
        # elif db_type.lower() == MSSQL_DB:
        #     query = f"INSERT INTO {self.config.table} ({self.column_list}) VALUES \n"
        else:
            raise ValueError(f"Unsupported DB type: {db_type}")

        params_list: List[List[str]] = []
        val_placeholders = []

        for record in records:
            row_params = []
            placeholders = []
            try:
                for i, bean in enumerate(self.column_format_list):
                    if (bean.data_type or "").lower() == DATA_IGNORE.lower():
                        continue
                    value = record[bean.id - 1] if (bean.id - 1) < len(record) else ""
                    value = self._format_value(value, bean)
                    placeholders.append("%s")
                    row_params.append(value)
                val_placeholders.append("(" + ", ".join(placeholders) + ")")
                params_list.append(row_params)
            except Exception as ex:
                self.logger.error(f"Record build error: {ex}", exc_info=True)
                return 1

        query += ",\n".join(val_placeholders) + ";"
        if self.config.show_query:
            self.logger.info(f"QUERY=>{query}<-")

        try:
            self.dao.exec_ins_query(query, params_list, self.config.show_query)
            self.logger.info("Insert query executed successfully.")
        except Exception as ex:
            self.logger.error(f"Insert failed: {ex}", exc_info=True)
            self.error_message = self.error_message or str(ex)
            return 1

        return 0

    def _format_value(self, value: str, bean: DBColumnMappingBean) -> str:
        """
        Mirrors SQLValueFormatter.formatValue() — basic implementation.
        Handles $FILE_NAME variable substitution; returns value as-is otherwise.
        """
        if bean.data_type == "variable" and bean.format:
            if "$FILE_NAME" in bean.format:
                return self.file_bean.fileName if self.file_bean else ""
        return value if value is not None else ""

    # -----------------------------------------------------------------------
    # Column list builder
    # -----------------------------------------------------------------------
    def _get_columns_list(self, format_list: List[DBColumnMappingBean]) -> str:
        """Mirrors LLMParser.getColumnsList()"""
        parts = []
        db_type = (self.config.sql or MYSQL_DB).lower()
        for bean in format_list:
            if (bean.data_type or "").lower() == DATA_IGNORE.lower():
                continue
            if db_type == MSSQL_DB:
                parts.append(f"[{bean.sql_column_name}]")
            else:
                parts.append(f"`{bean.sql_column_name}`")
        return ", ".join(parts)

    # -----------------------------------------------------------------------
    # HTTP helpers
    # -----------------------------------------------------------------------
    def _post_with_file(self, url: str, token: str, file_path: str,
                        form_data: dict, timeout: int) -> str:
        """Mirrors LLMParser.sendPostRequestWithFileInput()"""
        timeout = max(timeout, 30)
        self.logger.info(f"POST → {url}  file='{file_path}'")

        ext = os.path.splitext(file_path)[1].lower()
        media_types = {".pdf": "application/pdf", ".png": "image/png",
                       ".jpg": "image/jpeg", ".jpeg": "image/jpeg"}
        media_type = media_types.get(ext, "application/octet-stream")

        with open(file_path, "rb") as fh:
            files = {"files": (os.path.basename(file_path), fh, media_type)}

            # Attach example files if configured
            example_parts = self._get_example_file_parts()
            if example_parts:
                files.update(example_parts)

            headers  = {"Authorization": f"Bearer {token}"}
            resp     = requests.post(url, headers=headers, files=files,
                                     data=form_data, timeout=timeout)

        if not resp.ok:
            body = resp.text
            self.error_message = self.error_message or body
            self.logger.error(f"POST failed {resp.status_code}: {body}")
            resp.raise_for_status()

        self.logger.info(f"Response: {resp.text}")
        return resp.text

    def _get_example_file_parts(self) -> dict:
        """
        Mirrors LLMParser.createFileMap() — attaches example files if configured.
        """
        if not self.parser_id:
            return {}
        base_path = cfg("HACHIAI_LLM_EXAMPLE_FILES_BASE_PATH", "")
        if not base_path:
            return {}
        example_dir = os.path.join(base_path, self.parser_id)
        if not os.path.isdir(example_dir):
            return {}

        parts = {}
        for name in os.listdir(example_dir):
            full = os.path.join(example_dir, name)
            if not os.path.isfile(full):
                continue
            ext = os.path.splitext(name)[1].lower()
            if ext in (".pdf", ".png", ".jpeg", ".jpg", ".json"):
                parts[name] = open(full, "rb")
        return parts

    def _get_status_by_trace_id(self, trace_id: str) -> str:
        """Mirrors LLMParser.sendGetRequestWithTraceId()"""
        base_url = LLMConfig.status_api_url()
        url      = base_url + trace_id
        token    = LLMConfig.token()
        timeout  = LLMConfig.http_timeout()

        self.logger.info(f"GET → {url}")
        resp = requests.get(url, headers={"Authorization": f"Bearer {token}"},
                            timeout=max(timeout, 30))
        if not resp.ok:
            body = resp.text
            self.error_message = self.error_message or body
            self.logger.error(f"GET failed {resp.status_code}: {body}")
            resp.raise_for_status()

        self.logger.info(f"Response: {resp.text}")
        return resp.text

    # -----------------------------------------------------------------------
    # Parsing helpers  (mirrors static Util methods in Java)
    # -----------------------------------------------------------------------
    @staticmethod
    def _parse_task_detail(json_text: str) -> TaskDetail:
        d = json.loads(json_text)
        return TaskDetail(trace_id=d.get("trace_id", ""), wait_time_ms=d.get("wait_time_ms"))

    @staticmethod
    def _parse_task_response(json_text: str) -> TaskResponse:
        d = json.loads(json_text)
        return TaskResponse(
            status           = d.get("status", ""),
            value            = d.get("value", ""),
            requestResponse  = d.get("request_response", {}),
            total_pages      = int(d.get("total_pages", 0) or 0),
            pages_processed  = int(d.get("pages_processed", 0) or 0),
        )

    @staticmethod
    def _map_columns_to_values(prompt: str, db_columns: str,
                                remove_first: bool = False) -> Optional[Dict[str, str]]:
        """Mirrors Util.mapColumnsToValues()"""
        prompt_parts = [p.strip() for p in prompt.split("|")]
        column_parts = [c.strip() for c in db_columns.split(",")]

        if remove_first:
            prompt_parts = prompt_parts[1:]
            column_parts = column_parts[1:]

        length = min(len(prompt_parts), len(column_parts))
        return {column_parts[i]: prompt_parts[i] for i in range(length)}

    @staticmethod
    def _map_columns_to_values_full(prompt, db_columns, value_types, mandatory_values,
                                     keys, confidence_score, remove_first=False) -> List[Dict[str, str]]:
        """Mirrors Util.mapColumnsToValuesFull()"""
        def split_trim(s):
            return [x.strip() for x in s.split("|" if "|" in (s or "") else ",")] if s else []

        prompt_parts     = [p.strip() for p in (prompt or "").split("|")]
        column_parts     = [c.strip() for c in (db_columns or "").split(",")]
        type_parts       = [t.strip() for t in (value_types or "").split(",")]
        required_parts   = [r.strip() for r in (mandatory_values or "").split(",")]
        key_parts        = [k.strip() for k in (keys or "").split(",")]
        confidence_parts = [c.strip() for c in (confidence_score or "").split(",")]

        if remove_first:
            prompt_parts     = prompt_parts[1:]
            column_parts     = column_parts[1:]
            type_parts       = type_parts[1:] if type_parts else []
            required_parts   = required_parts[1:] if required_parts else []
            key_parts        = key_parts[1:] if key_parts else []
            confidence_parts = confidence_parts[1:] if confidence_parts else []

        length = min(len(prompt_parts), len(column_parts))
        rows   = []
        for i in range(length):
            rows.append({
                "Text":            prompt_parts[i],
                "Alias":           column_parts[i],
                "Type":            type_parts[i] if i < len(type_parts) and type_parts[i] else "string",
                "IsRequired":      required_parts[i] if i < len(required_parts) and required_parts[i] else "true",
                "Key":             key_parts[i] if i < len(key_parts) and key_parts[i] else "",
                "confidence_score": confidence_parts[i] if i < len(confidence_parts) and confidence_parts[i] else "",
            })
        return rows

    # -----------------------------------------------------------------------
    # DB query helpers
    # -----------------------------------------------------------------------
    def _get_select_query_results(self, query: str) -> list:
        try:
            self.logger.info(f"Select Query = {query}")
            return self.dao.run_query(query, None)
        except Exception as ex:
            self.logger.error(f"Query error: {ex}", exc_info=True)
            return []

    def _destroy(self) -> None:
        if self.dao:
            self.dao.close()
            self.dao = None


# ===========================================================================
# CLI entry point  (mirrors LLMParser.main())
# ===========================================================================
if __name__ == "__main__":
    import argparse
    import concurrent.futures
    import re

    ap = argparse.ArgumentParser(description="LLMParser")
    ap.add_argument("--config", required=True)
    ap.add_argument("--source", required=True)
    args = ap.parse_args()

    source = args.source
    is_dir = os.path.isdir(source)

    supported = re.compile(r"\.(pdf|png|jpg|jpeg)$", re.IGNORECASE)

    if is_dir:
        files = [
            os.path.abspath(os.path.join(source, f))
            for f in os.listdir(source)
            if supported.search(f) and os.path.isfile(os.path.join(source, f))
        ]
        max_workers = os.cpu_count() + 2
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            for f in files:
                executor.submit(LLMParser(args.config, f).run)
    else:
        LLMParser(args.config, source).run()