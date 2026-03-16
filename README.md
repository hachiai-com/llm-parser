# LLM Parser

A powerful, LLM-driven toolkit designed for automated data extraction from complex documents including **PDFs, PNGs, JPGs, and JPEGs**. This system leverages Large Language Models to dynamically identify document templates and extract structured information directly into a MySQL Database.

## What This Toolkit Does

- **Extract data from multiple formats**: Works with text-based PDFs and image snapshots (PNG/JPG).
- **Dynamic Template Identification**: Automatically recognizes the document type (e.g., specific vendor invoice) from your database without manual selection.
- **LLM-Powered Extraction**: Uses sophisticated prompts to extract common fields and complex table data.
- **Database Integration**: Seamlessly inserts extracted records into configured MySQL tables.
- **Asynchronous Processing**: Handles long-running LLM tasks with an efficient polling mechanism.
- **Detailed Traceability**: Generates unique execution IDs and per-file status reports for full observability.

---

## Quick Start

### Prerequisites

- Python 3.10+
- MySQL Database
- Access to HachiAI LLM APIs

### Installation

1. Clone the repository.
2. Install dependencies:
```bash
   pip install -r requirements.txt
```

### Setup

1. **Create your environment file**:
   Copy `.env.example` to `.env`:
```bash
   cp .env.example .env
```

2. **Configure Variables**:
   Update `.env` with your database credentials and **HACHIAI_LLM_TOKEN**. (See [Configuration](#configuration) for details).

---

## How to Use

The toolkit accepts JSON input via standard input and returns JSON output. It is designed for integration with AI agents, automation systems, or standalone CLI usage.

### Basic Usage

**Input Format:**
```json
{
  "capability": "capability_name",
  "args": {
    "config": "numeric_id_or_path",
    "source": "path/to/file_or_directory",
    "env_file": "path/to/.env"
  }
}
```

**Execution:**
```bash
echo '{"capability": "llm_parser", "args": {"config": "123", "source": "docs/inv.pdf", "env_file": "/path/to/.env"}}' | python main.py
```

---

### Capability 1: Dynamic Template Parser

Identifies the appropriate parser template from the database based on document content, then extracts information.

**Required Parameters:**
| Parameter | Type | Description |
| :--- | :--- | :--- |
| `config` | string | Numeric parser ID or path to a config JSON file |
| `source` | string | Full path to the source file or directory containing documents |
| `env_file` | string | Full path to the `.env` file containing credentials for this client |

**Example:**
```json
{
  "capability": "dynamic_template_llm_parser",
  "args": {
    "config": "234",
    "source": "./invoices/january/",
    "env_file": "/path/to/client_a/.env"
  }
}
```

> **Using a config file instead of a numeric ID?**
> See [`templates/dynamic_template_llm_parser_config.json`](./templates/dynamic_template_llm_parser_config.json) for the full reference structure of the config file.

**Success Response:**
```json
{
  "result": {
    "status": "success",
    "message": "All 1 file(s) processed successfully.",
    "parser_file_execution_id": "exec_abc123",
    "total_files": 1,
    "completed_files": 1,
    "files": [
      {
        "file_name": "invoice_001.pdf",
        "status": "Completed",
        "matched_vendor": "Vendor Name Inc.",
        "duration_ms": 4500
      }
    ]
  },
  "capability": "dynamic_template_llm_parser"
}
```

**Error Response:**
```json
{
  "error": "No supported files found to process.",
  "capability": "dynamic_template_llm_parser"
}
```

---

### Capability 2: Specific LLM Parser

Parses documents using a specific, pre-defined LLM template parser configuration.

**Required Parameters:**
| Parameter | Type | Description |
| :--- | :--- | :--- |
| `config` | string | Numeric parser ID (e.g., "123") or path to a config JSON file |
| `source` | string | Path to the specific file or directory to be parsed |
| `env_file` | string | Full path to the `.env` file containing credentials for this client |

**Example:**
```json
{
  "capability": "llm_parser",
  "args": {
    "config": "123",
    "source": "docs/specific_invoice.pdf",
    "env_file": "/path/to/client_a/.env"
  }
}
```

> **Using a config file instead of a numeric ID?**
> See [`templates/llm_parser_config.json`](./templates/llm_parser_config.json) for the full reference structure of the config file.

**Success Response:**
```json
{
  "result": {
    "status": "success",
    "message": "File processed successfully",
    "parser_execution_id": "PE-789",
    "parser_name": "Standard Invoice Parser",
    "total_pages": 1,
    "records_processed": 5,
    "duration_ms": 3200
  },
  "capability": "llm_parser"
}
```

**Error Response:**
```json
{
  "error": "Parser failed with exit code 1.",
  "capability": "llm_parser"
}
```

---

### Standalone CLI Execution

The parsers can also be executed directly as Python scripts. In this case the `.env` file is loaded automatically from the working directory or script directory — no `env_file` argument is needed.
```bash
# Run the Dynamic Parser
python dynamic_template_llm_parser.py --config 234 --source ./invoices/

# Run a specific LLM Parser
python llm_parser.py --config 123 --source ./docs/invoice_001.pdf
```

---

## Config File Templates

If you prefer to pass a config JSON file path instead of a numeric parser ID, reference templates are provided in the [`templates/`](./templates/) folder:

| File | Used With |
| :--- | :--- |
| [`templates/llm_parser_config.json`](./templates/llm_parser_config.json) | `llm_parser` capability |
| [`templates/dynamic_template_llm_parser_config.json`](./templates/dynamic_template_llm_parser_config.json) | `dynamic_template_llm_parser` capability |

These files document every supported field and its expected value. Copy the relevant file, fill in your values, and pass its path as the `config` argument.

---

## Configuration

### Environment File (`env_file`)

Every call to the toolkit requires an `env_file` parameter pointing to a `.env` file. This allows different clients or environments to use their own credentials without any code changes.

### Required Variables

You must set these core variables in your `.env` file for the toolkit to function:

| Variable | Description |
| :--- | :--- |
| `DATABASE_HOST` | MySQL server host name |
| `DATABASE_USERNAME` | MySQL database username |
| `DATABASE_PASSWORD` | MySQL database password |
| `DATABASE_NAME` | Target database name |
| `HACHIAI_LLM_TOKEN` | **Mandatory**: Your HachiAI API security token |
| `HACHIAI_LLM_API` | Base URL for the LLM extraction API |

A full list of all supported variables with their defaults can be found in `.env.example`.

---

## Example AI Agent Prompts

### Example 1: Batch Process a Folder
`"Use Dynamic Template LLM Parser for parsing. Use source as 'D:/llm-learning', while config and environment files are attached."`

### Example 2: Parse a Specific File
`"Extract data using LLM parser from 'inv_99.pdf' using the specific parser ID 123 and the env file at /projects/client_b/.env."`

---

## Logging & Observability

Logs are stored in the `logs/` directory. Each execution tracks:
- **parser_file_execution_id**: Unique ID for the entire run.
- **Trace IDs**: `qna_trace_id` for individual LLM API calls.
- **Status States**:
    - `Completed`: Successful extraction and DB insertion.
    - `Unapproved`: Document content did not match any vendor template.
    - `Failed`: Template matched but extraction or DB insertion failed.
    - `Waiting`: LLM API did not return a result within the polling window.

---

## Important Notes

- **env_file is mandatory**: Every toolkit call must include a valid `env_file` path. If the file does not exist, the toolkit returns an error immediately before any processing begins.
- **Database Dependency**: A valid MySQL connection is required to fetch templates and save results.
- **Output**: The toolkit primarily writes to the database; summary results are returned via JSON.