from pydantic import BaseModel
from typing import List, Optional, Any

class PaperSummary(BaseModel):
    title: Optional[str]
    authors: Optional[List[str]]
    abstract: Optional[str]
    objective: Optional[str]
    methodology: Optional[str]
    dataset: Optional[str]
    model: Optional[str]
    experiments: Optional[str]
    key_findings: Optional[Any]
    limitations: Optional[str]
    future_scope: Optional[str]
    final_summary: Optional[str]
