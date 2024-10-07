from pydantic import BaseModel
from typing import Optional

class FetchParams(BaseModel):
    start_row: int
    end_row: int
    sort: Optional[str] = None