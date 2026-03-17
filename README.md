# LLM Parser

A powerful, LLM-driven toolkit for automated data extraction from complex documents including **PDFs, PNGs, JPGs, and JPEGs**. This system leverages Large Language Models to dynamically identify document templates and extract structured information directly into a MySQL Database.

---

## What This Toolkit Does

- **Extract data from multiple formats** — Works with text-based PDFs and image snapshots (PNG/JPG/JPEG).
- **Dynamic Template Identification** — Automatically recognizes the document type (e.g., a specific vendor invoice) from your database without manual selection.
- **LLM-Powered Extraction** — Uses sophisticated prompts to extract common fields and complex table data.
- **Database Integration** — Seamlessly inserts extracted records into configured MySQL tables.
- **Asynchronous Processing** — Handles long-running LLM tasks with an efficient polling mechanism.
- **Detailed Traceability** — Generates unique execution IDs and per-file status reports for full observability.

---

## Capabilities

### Capability 1: Dynamic Template Parser

Automatically identifies the appropriate parser template from your database based on document content, then extracts the information — no manual template selection needed.

**Parameters:**

| Parameter | Type | Description |
| :--- | :--- | :--- |
| `config` | string | Numeric parser ID or path to a config JSON file |
| `source` | string | Full path to the source file or directory containing documents |

**Example Input:**
```json
{
  "capability": "dynamic_template_llm_parser",
  "args": {
    "config": "234",
    "source": "./invoices/january/"
  }
}
```

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

> For config file usage, see [`templates/dynamic_template_llm_parser_config.json`](./templates/dynamic_template_llm_parser_config.json).

---

### Capability 2: Specific LLM Parser

Parses documents using a specific, pre-defined LLM template parser configuration. Use this when you already know the exact parser to apply.

**Parameters:**

| Parameter | Type | Description |
| :--- | :--- | :--- |
| `config` | string | Numeric parser ID (e.g., `"123"`) or path to a config JSON file |
| `source` | string | Path to the specific file or directory to be parsed |

**Example Input:**
```json
{
  "capability": "llm_parser",
  "args": {
    "config": "123",
    "source": "docs/specific_invoice.pdf"
  }
}
```

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

> For config file usage, see [`templates/llm_parser_config.json`](./templates/llm_parser_config.json).

---

## Required Configuration

> ⚠️ **These variables are mandatory.** The toolkit will not function without a valid MySQL connection and a HachiAI API token. All variables below must be set before running.

| Variable | Required | Description |
| :--- | :---: | :--- |
| `LLM_PARSER_DATABASE_HOST` | ✅ | MySQL server host name |
| `LLM_PARSER_DATABASE_USERNAME` | ✅ | MySQL database username |
| `LLM_PARSER_DATABASE_PASSWORD` | ✅ | MySQL database password |
| `LLM_PARSER_DATABASE_NAME` | ✅ | Target database name |
| `LLM_PARSER_HACHIAI_LLM_TOKEN` | ✅ | Your HachiAI API security token |
| `LLM_PARSER_HACHIAI_LLM_API` | ✅ | Base URL for the LLM extraction API |

A full list of all supported variables with their defaults can be found in `.env.example`.

---

## 🖥️ Option 1: Run Locally

### Prerequisites

- Python 3.10+
- MySQL Database
- Access to HachiAI LLM APIs

### Step 1 — Clone the Repository

```bash
git clone <repository-url>
cd llm-parser
```

### Step 2 — Install Dependencies

```bash
pip install -r requirements.txt
```

### Step 3 — Set Up Environment File

Copy the example environment file:

```bash
# Mac/Linux
cp .env.example .env

# Windows
copy .env.example .env
```

Then open `.env` and fill in all the [required variables](#required-configuration) listed above.

### Step 4 — Run the Parser

**Via Echo (JSON input):**

```bash
# Mac/Linux
echo '{"capability": "llm_parser", "args": {"config": "123", "source": "docs/inv.pdf"}}' | python main.py

# Windows
echo {"capability": "llm_parser", "args": {"config": "123", "source": "docs\inv.pdf"}} | python main.py
```

**Via CLI directly:**

```bash
# Run the Dynamic Template Parser
python dynamic_template_llm_parser.py --config 234 --source ./invoices/

# Run a Specific LLM Parser
python llm_parser.py --config 123 --source ./docs/invoice_001.pdf
```

---

## 🤖 Option 2: Use with an AI Agent

No cloning or local setup needed. Configure your environment variables directly in the AI platform, then prompt the agent to run the parser.

### Step 1 — Add Environment Variables

1. Go to **User Settings**
2. Open the **Environment Variables** tab
3. Add each of the following variables:

Add all the [required variables](#required-configuration) listed above.

### Step 2 — Prompt the Agent

Simply tell the AI agent what you want to parse. Here are ready-to-use example prompts:

**Batch process a folder:**
> *"Use Dynamic Template LLM Parser for parsing. Use source as 'D:/llm-learning', while config id is 234."*

**Parse a specific file:**
> *"Extract data using LLM parser from 'inv_99.pdf' using the specific parser ID 123."*

---

## Capabilities

### Dynamic Template Parser
Automatically identifies the right template from your database based on document content, then extracts data.

```json
{
  "capability": "dynamic_template_llm_parser",
  "args": {
    "config": "234",
    "source": "./invoices/january/"
  }
}
```

### Specific LLM Parser
Parses documents using a specific, pre-defined template configuration.

```json
{
  "capability": "llm_parser",
  "args": {
    "config": "123",
    "source": "docs/specific_invoice.pdf"
  }
}
```

> For config file usage, see the [`templates/`](./templates/) folder for reference JSON structures.

---

## Logging & Observability

Logs are stored in the `logs/` directory. Each execution tracks a unique `parser_file_execution_id` and per-file status:

| Status | Meaning |
| :--- | :--- |
| `Completed` | Successful extraction and DB insertion |
| `Unapproved` | Document did not match any vendor template |
| `Failed` | Template matched but extraction or DB insertion failed |
| `Waiting` | LLM API did not return a result within the polling window |

---

## Important Notes

- **Database Dependency**: A valid MySQL connection is required to fetch templates and save results.
- **Output**: The toolkit primarily writes extracted data to the database; summary results are returned via JSON.