class BaseModel:
    pass


class User(BaseModel):
    def __init__(self, id: str, api_key: str):
        self.id = id
        self.api_key = api_key


user = User(
    id="U000000000000",
    api_key="00000000-0000-0000-0000-000000000000",
)
