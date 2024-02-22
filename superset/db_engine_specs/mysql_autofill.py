from __future__ import annotations

from sqlalchemy import create_engine
from urllib import parse
from superset.databases.utils import make_url_safe
from typing import Any, TypedDict, Optional

from flask_babel import gettext as __
from marshmallow import fields, Schema
from sqlalchemy.engine.url import URL

from superset.db_engine_specs.mysql import MySQLEngineSpec
from superset.errors import ErrorLevel, SupersetError, SupersetErrorType
from superset.utils.network import is_hostname_valid, is_port_open

DATABASE_NAME = "database_try"
HOST_NAME = "localhost"
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
    sqlalchemy_uri_placeholder = (
        "mysql://user:password@localhost:3306/database_try[?key=value&key=value]"
    )

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
                f"{cls.engine}+{cls.engine_name}+{cls.default_driver}".rstrip("+"),
                username=parameters.get("username"),
                password=parameters.get("password"),
                host=HOST_NAME,
                port=PORT_NAME,
                database=DATABASE_NAME
            )
        )

    @classmethod
    def adjust_engine_params(
        cls,
        uri: URL,
        connect_args: dict[str, Any],
        catalog: Optional[str] = None,
        schema: Optional[str] = None,
    ) -> tuple[URL, dict[str, Any]]:
        uri, new_connect_args = super().adjust_engine_params(
            uri,
            connect_args,
            catalog,
            schema,
        )

        if schema:
            uri = uri.set(database=parse.quote(schema, safe=""))

        return uri, new_connect_args

    @classmethod
    def get_parameters_from_uri(  # pylint: disable=unused-argument
        cls, uri: str, encrypted_extra: dict[str, Any] | None = None
    ) -> MySQLAutoParametersType:
        url = make_url_safe(uri)
        return {
            "username": url.username,
            "password": str(url.password or ""),
        }


    @classmethod
    def validate_parameters(cls, properties: MySQLAutoPropertiesType) -> list[SupersetError]:
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

        username = parameters.get("username", "")
        password = parameters.get("password", "")
        host = HOST_NAME
        database = DATABASE_NAME
        port = PORT_NAME

        host = HOST_NAME
        port = PORT_NAME

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
            url = "mysql://{}:{}@{}:{}/{}".format(username, password, host, port, database)
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
