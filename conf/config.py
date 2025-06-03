import os
# from dotenv import load_dotenv
# from pathlib import Path

current_dir = os.path.dirname(os.path.abspath(__file__))
env_path = os.path.join(current_dir, '.env')
parent_dir = os.path.abspath(os.path.join(current_dir, os.pardir))
# load_dotenv(Path(env_path))


import yaml
import urllib
import warnings
from typing import Any
from typing import Literal

from pydantic import (
    BaseModel,
    PostgresDsn,
    computed_field,
    model_validator,
)
from typing_extensions import Self
from pydantic import BaseModel
from pydantic_core import MultiHostUrl
from pydantic_settings import BaseSettings, SettingsConfigDict


class ENVConfig(BaseSettings):
    model_config = SettingsConfigDict(
        # Use top level .env file (one level above ./backend/)
        env_file=env_path,
        env_ignore_empty=True,
        extra="ignore",
    )

    ENVIRONMENT: Literal["local", "staging", "production"] = "local"

    # postgres
    POSTGRES_SERVER: str
    POSTGRES_PORT: int = 5432
    POSTGRES_USER: str
    POSTGRES_PASSWORD: str
    POSTGRES_DB: str 

    @computed_field
    @property
    def POSTGRES_URL(self) -> Any:
        return f"jdbc:postgresql://{self.POSTGRES_SERVER}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
    
    @computed_field  
    @property
    def POSTGRES_URL_ENGINE(self) -> Any:
        return f'postgresql://{self.POSTGRES_USER}:password@{self.POSTGRES_SERVER}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}?password={self.POSTGRES_PASSWORD}'


    # clickhouse
    CLICKHOUSE_USER: str
    CLICKHOUSE_PASSWORD: str
    CLICKHOUSE_SERVER: str
    CLICKHOUSE_PORT: int = 8123
    CLICKHOUSE_STAG_DB: str
    CLICKHOUSE_PROD_DB: str
    
    @computed_field
    @property
    def CLICKHOUSE_PROD_URL(self) -> Any:
        return f"jdbc:clickhouse://{self.CLICKHOUSE_SERVER}:{self.CLICKHOUSE_PORT}/{self.CLICKHOUSE_PROD_DB}"

    @computed_field
    @property
    def CLICKHOUSE_STAG_URL(self) -> Any:
        return f"jdbc:clickhouse://{self.CLICKHOUSE_SERVER}:{self.CLICKHOUSE_PORT}/{self.CLICKHOUSE_STAG_DB}"
    
    # kafka 
    KAFKA_USER: str
    KAFKA_PASSWORD: str
    
    def _check_default_secret(self, var_name: str, value: str | None) -> None:
        if value == "":
            message = (
                f'The value of {var_name} is null,'
                "for develop, please fill it, at least for deployments."
            )
            if self.ENVIRONMENT == "local":
                warnings.warn(message, stacklevel=1)
            else:
                raise ValueError(message)

    @model_validator(mode="after")
    def _enforce_non_default_secrets(self) -> Self:
        self._check_default_secret("POSTGRES_SERVER", self.POSTGRES_SERVER)
        self._check_default_secret("POSTGRES_PORT", self.POSTGRES_PORT)
        self._check_default_secret("POSTGRES_USER", self.POSTGRES_USER)
        self._check_default_secret("POSTGRES_PASSWORD", self.POSTGRES_PASSWORD)
        self._check_default_secret("POSTGRES_DB", self.POSTGRES_DB)
        self._check_default_secret("POSTGRES_URL", self.POSTGRES_URL)
        self._check_default_secret("POSTGRES_URL_ENGINE", self.POSTGRES_URL_ENGINE)
        self._check_default_secret("KAFKA_USER", self.KAFKA_USER)
        self._check_default_secret("KAFKA_PASSWORD", self.KAFKA_PASSWORD)

        return self


class EventConfig(BaseModel):
    app_name: str
    kafka_bootstrap_servers: str
    kafka_topic_pattern: str
    kafka_security_protocol: str 
    kafka_sasl_mechanism: str
    postgres_driver: str
    clickhouse_driver: str
    num_process: int
    num_partition: int
    batch_duration: int
    spark_jars_packages: str
    postgres_resource: str
    clickhouse_resource: str
    stag_checkpoint_path: str
    prod_checkpoint_path: str
    checkpoint_path: str
    logs_path: str
    

class PathConfig(BaseModel):
    checkpoint_location_stag: str
    checkpoint_location_prod: str
    checkpoint_location: str
    logs_folder: str
    postgres_resource_path: str
    clickhouse_resource_path: str

class Config(BaseModel):
    env: ENVConfig
    event: EventConfig
    path: PathConfig

    @classmethod
    def load_config(cls, yaml_file_path: str):

        with open(yaml_file_path, 'r') as yaml_file:
            yaml_config = yaml.safe_load(yaml_file)

        event_config = EventConfig(**yaml_config.get('event', {}))
        checkpoint_location_stag = os.path.join(parent_dir, event_config.stag_checkpoint_path)
        checkpoint_location_prod = os.path.join(parent_dir, event_config.prod_checkpoint_path)
        checkpoint_location = os.path.join(parent_dir, event_config.checkpoint_path)
        logs_folder = os.path.join(parent_dir, event_config.logs_path)
        postgres_resource_path = os.path.join(parent_dir, event_config.postgres_resource)
        clickhouse_resource_path = os.path.join(parent_dir, event_config.clickhouse_resource)
        path_config = PathConfig(
            checkpoint_location_stag=checkpoint_location_stag, 
            checkpoint_location_prod=checkpoint_location_prod, 
            checkpoint_location=checkpoint_location,
            logs_folder=logs_folder, 
            postgres_resource_path=postgres_resource_path,
            clickhouse_resource_path=clickhouse_resource_path
        )
        env_config = ENVConfig()

        return cls(env=env_config, event=event_config, path=path_config)

config_path = os.path.join(current_dir, 'config.yaml')
config_app = Config.load_config(config_path)
