from __future__ import annotations

from sqlalchemy import create_engine
from superset.databases.utils import make_url_safe
from typing import Any, TypedDict

from flask_babel import gettext as __
from marshmallow import fields, Schema
from sqlalchemy.engine.url import URL

from superset.db_engine_specs.mysql import MySQLEngineSpec
from superset.errors import ErrorLevel, SupersetError, SupersetErrorType
from superset.utils.network import is_port_open

DATABASE_NAME = "database_try"
HOST_NAME = "127.0.0.1"
PORT_NAME = 3306


class MySQLAutoParametersSchema(Schema):
    username = fields.String(
        required=True,
        allow_none=True,
        metadata={"description": __("Username")},
    )
    password = fields.String(
        required=True,
        allow_none=True,
        metadata={"description": __("Password")},
    )


class MySQLAutoParametersType(TypedDict):
    username: str | None
    password: str | None


class MySQLAutoPropertiesType(TypedDict):
    parameters: MySQLAutoParametersType


class MySQLAutoEngineSpec(MySQLEngineSpec):
    """Custom Engine for MySQL Auto Connection"""

    engine_name = "MySQL Auto"
    disable_ssh_tunneling = True
    parameters_schema = MySQLAutoParametersSchema()

    @classmethod
    def build_sqlalchemy_uri(
        cls,
        parameters: MySQLAutoParametersType,
        encrypted_extra: dict[str, str] | None = None,
    ) -> str:
        return str(
            URL.create(
                f"{cls.engine}+{cls.default_driver}".rstrip("+"),
                username=parameters.get("username"),
                password=parameters.get("password"),
                host=HOST_NAME,
                port=PORT_NAME,
                database=DATABASE_NAME
            )
        )

    @classmethod
    def get_parameters_from_uri(  # pylint: disable=unused-argument
        cls, uri: str, encrypted_extra: dict[str, Any] | None = None
    ) -> MySQLAutoParametersType:
        url = make_url_safe(uri)
        return {
            "username": url.username,
            "password": str(url.password or "")
        }

    @classmethod
    def validate_parameters(cls, properties: MySQLAutoPropertiesType) -> list[
        SupersetError]:
        """
        Validates parameters for a MySQL connection.
        """
        errors: list[SupersetError] = []

        parameters = properties.get("parameters", {})


        required = {"username", "password"}

        missing_params = required - set(parameters.keys())
        if missing_params:
            errors.append(
                SupersetError(
                    message=f"One or more parameters are missing: {', '.join(missing_params)}",
                    error_type=SupersetErrorType.CONNECTION_MISSING_PARAMETERS_ERROR,
                    level=ErrorLevel.WARNING,
                    extra={"missing": list(missing_params)},
                )
            )

        parameters.setdefault("host", HOST_NAME)
        parameters.setdefault("port", str(PORT_NAME))
        parameters.setdefault("database", DATABASE_NAME)

        username = parameters.get("username", "")
        password = parameters.get("password", "")
        host = parameters["host"]
        port = parameters["port"]
        database = parameters["database"]


        if not is_port_open(host, port):
            errors.append(
                SupersetError(
                    message="The port is closed.",
                    error_type=SupersetErrorType.CONNECTION_PORT_CLOSED_ERROR,
                    level=ErrorLevel.ERROR,
                    extra={"invalid": ["port"]},
                ),
            )

        # Attempting to connect to MySQL
        try:
            # Assuming MySQL is running locally on default port 3306
            url = f"mysql://{username}:{password}@{host}:{port}/{database}"

            # Attempt to establish a connection
            engine = create_engine(url)
            engine.connect()
        except Exception as e:
            errors.append(
                SupersetError(
                    message=f"Error connecting to MySQL database: {str(e)}",
                    error_type=SupersetErrorType.CONNECTION_ACCESS_DENIED_ERROR,
                    level=ErrorLevel.ERROR,
                )
            )

        return errors
