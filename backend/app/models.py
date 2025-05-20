from pydantic import BaseModel, Field
from typing import List, Optional

class PowerPlant(BaseModel):
    """
    Model representing a power plant with its annual net generation
    """
    id: str = Field(..., description="Power plant ID")
    name: str = Field(..., description="Power plant name")
    state: str = Field(..., description="State abbreviation")
    netGeneration: float = Field(..., description="Annual net generation in MWh") 