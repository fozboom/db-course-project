from pydantic import BaseModel, Field


class CartItem(BaseModel):
    product_id: str
    quantity: int = Field(gt=0)


class CartItemUpdate(BaseModel):
    quantity: int = Field(ge=0)
