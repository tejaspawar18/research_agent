from pydantic import BaseModel, field_validator
from typing import List, Optional, Any

class PaperSummary(BaseModel):
    title: Optional[str]
    doi: Optional[str]
    published_date: Optional[str]   
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
    input_token: Optional[int] = None
    output_token: Optional[int] = None

    @field_validator('objective', 'methodology', 'dataset', 'model', 'experiments', 'key_findings', 'limitations', 'future_scope', 'final_summary', mode='before')
    @classmethod
    def convert_list_to_str(cls, v):
        if isinstance(v, list):
            return "\n".join(map(str, v))
        return v
