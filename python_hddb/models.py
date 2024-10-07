from pydantic import BaseModel

class FetchParams(BaseModel):
    start_row: int
    end_row: int