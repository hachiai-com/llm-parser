from typing import List, Optional, Any
from pydantic import Field, BaseModel, ConfigDict
from constants import TaskExecutionStatus


class TaskResponse(BaseModel):
    """
    Represents the response returned from the LLM task execution service.

    Contains task status, processing step, extracted value, task metadata,
    request/response payloads, and page-level processing statistics.
    """

    model_config = ConfigDict(extra="ignore", populate_by_name=True)  # mirrors @JsonIgnoreProperties(ignoreUnknown = true)

    status: Optional[str] = Field(
        None,
        description="Execution status of the task returned by the LLM service."
    )

    step: Optional[str] = Field(
        None,
        description="Current processing step of the task execution workflow."
    )

    value: Optional[str] = Field(
        None,
        description="Extracted value or output returned by the task."
    )

    task_type: Optional[str] = Field(
        None,
        description="Type of task executed by the LLM pipeline."
    )

    required_raw_text: Optional[str] = Field(
        None,
        description="Raw text extracted from the document required for task processing."
    )

    files: Optional[List[str]] = Field(
        None,
        description="List of file names associated with the task execution."
    )

    retries: Optional[int] = Field(
        None,
        description="Number of retry attempts made for the task execution."
    )

    requestResponse: Optional[Any] = Field(
        None,
        alias="request_response",
        description="Raw request/response payload returned by the LLM API."
    )

    total_pages: Optional[int] = Field(
        None,
        description="Total number of pages detected in the document."
    )

    pages_processed: Optional[int] = Field(
        None,
        description="Number of pages that have been processed so far."
    )

    def get_required_raw_text(self) -> str:
        """Returns cleaned raw text. Mirrors Java getRequiredRawText()."""
        if not self.required_raw_text or not self.required_raw_text.strip():
            return "No raw text available"
        return self.required_raw_text.strip()

    def is_failed(self) -> bool:
        """Returns True if the task execution status is failed."""
        return self.status and self.status.lower() == TaskExecutionStatus.failed.value

    def is_fulfilled(self) -> bool:
        """Returns True if the task execution status is fulfilled."""
        return self.status and self.status.lower() == TaskExecutionStatus.fulfilled.value