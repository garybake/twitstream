from pydantic import BaseModel
from pydantic import confloat
from pydantic import StrictStr


class ConsumerResponse(BaseModel):
    topic: StrictStr
    key: str
    value: str
    timestamp: str
