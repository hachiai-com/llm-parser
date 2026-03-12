from typing import Optional
from pydantic import Field, BaseModel
from models.task_response import TaskResponse


class ExecutionData(BaseModel):
    """
    Stores runtime execution metadata for a dynamic LLM parser run.

    Tracks parser execution identifiers, trace IDs, conversation input,
    matched parser information, and any errors that occur during processing.
    """

    parserFileExecutionId: Optional[str] = Field(
        None,
        description="Unique execution identifier generated for each parser run."
    )

    textFromConversationAPI: Optional[str] = Field(
        None,
        description="Raw text received from the Conversation API used as input for parsing."
    )

    conversationTraceId: Optional[str] = Field(
        None,
        description="Trace ID associated with the Conversation API request for debugging and observability."
    )

    qnaTraceTraceId: Optional[str] = Field(
        None,
        description="Trace ID associated with the QnA or LLM processing workflow."
    )

    parserId: Optional[str] = Field(
        None,
        description="Identifier of the parser configuration selected for execution."
    )

    matchedParserId: Optional[str] = Field(
        None,
        description="Identifier of the dynamically matched parser template used during execution."
    )

    errorMessage: Optional[str] = Field(
        None,
        description="Error message captured if the parser execution fails."
    )

    taskResponse: Optional["TaskResponse"] = Field(
        None,
        description="Structured response returned after executing the parser task."
    )