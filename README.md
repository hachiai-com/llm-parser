# LLM Parser: Intelligent Document Management System (IDMS)

A powerful, LLM-driven toolkit designed for automated data extraction from complex documents including **PDFs, PNGs, JPGs, and JPEGs**. This system leverages Large Language Models to dynamically identify document templates and extract structured information into MySQL Database.

## 🚀 Key Features

- **Multi-Format Support**: Process text-heavy PDFs or image-based snapshots (PNG/JPG).
- **Dynamic Template Identification**: Automatically recognizes the document type (e.g., specific vendor invoice) without manual configuration.
- **LLM-Powered Extraction**: Uses sophisticated prompts to extract both common fields and complex table data.
- **SQL Integration**: Seamlessly inserts extracted data into configured database tables.
- **Async Processing**: Efficient polling mechanism for long-running LLM tasks.
- **Robust Logging**: Detailed per-instance logging tracking every step of the extraction process.

## 🛠️ Getting Started

### Prerequisites

- Python 3.8+
- MySQL Database
- Access to HachiAI LLM APIs

### Installation

1. Clone the repository.
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

### Configuration

Copy `.env.example` to `.env` and configure your settings:

```bash
# Database
DATABASE_TYPE=mysql
DATABASE_HOST=localhost
DATABASE_NAME=idms_db

# LLM API
HACHIAI_LLM_TOKEN=your_secure_token
HACHIAI_LLM_API=https://api.hachiai.com/v1/llm/qna/async
```

## 🧩 Toolkit Capabilities

Defined in `toolkit.json`, this project exposes two primary capabilities:

### 1. `dynamic_template_llm_parser`
Dynamically identifies the appropriate parser template from the database based on document content, then extracts information.

**Payload Schema:**
```json
{
  "capability": "dynamic_template_llm_parser",
  "args": {
    "config": "numeric_parser_id_or_config_path",
    "source": "/path/to/document_or_directory"
  }
}
```

### 2. `llm_parser`
Parses documents using a specific, pre-defined LLM template parser configuration.

**Payload Schema:**
```json
{
  "capability": "llm_parser",
  "args": {
    "config": "numeric_parser_id",
    "source": "/path/to/single_file"
  }
}
```

## 💻 Usage

### Via Main Entrypoint (JSON API)
Run the toolkit passing a JSON payload through standard input:
```bash
echo '{"capability": "llm_parser", "args": {"config": "123", "source": "docs/inv.pdf"}}' | python main.py
```

### Standalone CLI Execution
Both parsers can be run directly:
```bash
# Dynamic Parser
python dynamic_template_llm_parser.py --config 234 --source ./invoices/

# specific LLM Parser
python llm_parser.py --config 123 --source ./docs/invoice_001.pdf
```

## 📊 Logging & Observability

Logs are stored in the `logs/` directory. Each execution generates a unique `parserFileExecutionId`, allowing for detailed traceability of every document processed. The system tracks:
-  Unapproved = "When the template does not match the document"
-  Failed     = "When the template matches the document but the extraction fails"
-  Completed  = "When the template matches the document and the extraction is successful"

---
© 2026 [HachiAI](https://hachiai-com). Intelligent Automation for the Modern Enterprise.
