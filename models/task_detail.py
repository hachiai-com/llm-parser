from typing import Optional
from pydantic import Field, BaseModel, ConfigDict
from constants import TaskExecutionStatus


class TaskDetail(BaseModel):
    """
    Represents execution metadata for an individual task in the LLM
    processing pipeline. Tracks task identifiers, execution status,
    task type, and the wait time before the task execution started.
    
    Mirrors com.uxplore.utils.llm.bean.TaskDetail
    """

    model_config = ConfigDict(extra="ignore")  # mirrors @JsonIgnoreProperties(ignoreUnknown = true)

    task_id: Optional[str] = Field(
        None,
        description="Unique identifier of the task."
    )

    trace_id: Optional[str] = Field(
        None,
        description="Trace identifier used to correlate task execution across services."
    )

    status: Optional[str] = Field(
        None,
        description="Execution status of the task returned by the async LLM API."
    )

    task_type: Optional[str] = Field(
        None,
        description="Type of task being executed within the processing pipeline."
    )

    wait_time_ms: Optional[int] = Field(
        None,
        description="Time in milliseconds the task waited before execution."
    )

    def is_failed(self) -> bool:
        """Returns True if the task execution status is FAILED."""
        return self.status and self.status.lower() == TaskExecutionStatus.failed.value

    def is_fulfilled(self) -> bool:
        """Returns True if the task execution status is FULFILLED."""
        return self.status and self.status.lower() == TaskExecutionStatus.fulfilled.value