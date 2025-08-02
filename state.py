from typing import TypedDict, Dict, Any

class AgentState(TypedDict):
    target_bank: str
    pdf_path: str
    csv_path: str
    analysis: str
    current_code: str
    error_message: str
    attempt: int
    max_attempts: int
    success: bool
    plan: Dict[str, Any]
    reflection: str
