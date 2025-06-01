# apps/exchange/settings.py
from functools import lru_cache
from pydantic import Field
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    mongo_host: str = Field("localhost", env="MONGO_HOST")
    mongo_port: int = Field(27017,       env="MONGO_PORT")
    mongo_user: str = Field("",          env="MONGO_USER")
    mongo_pass: str = Field("",          env="MONGO_PASS")
    mongo_db:   str = Field("exchange",  env="MONGO_DB")

    mcast_group: str = Field("224.1.1.1", env="MCAST_GROUP")
    mcast_port:  int = Field(4444,        env="MCAST_PORT")

    class Config:
        env_file = ".env"

    def show(self):
        return f"mongo={self.mongo_host}:{self.mongo_port}/{self.mongo_db} mcast={self.mcast_group}:{self.mcast_port}"

@lru_cache
def get_settings() -> Settings:
    return Settings()
