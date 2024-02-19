from __future__ import annotations

from superset.databases.utils import make_url_safe
from superset.db_engine_specs.base import BasicParametersMixin
import logging
from typing import Any, TypedDict

from flask_babel import gettext as __
from marshmallow import fields, Schema
from sqlalchemy.engine.url import URL

from superset.db_engine_specs.mysql import MySQLEngineSpec
from superset.errors import ErrorLevel, SupersetError, SupersetErrorType

_logger = logging.getLogger()


class MySQLAutoParametersSchema(Schema):
    username = fields.String(
        required=True,
        allow_none=True,
        metadata={"description": __("Username")},
    )
    password = fields.String(
        required=False,
        allow_none=True,
        metadata={"description": __("Password")},
    )


class MySQLAutoParametersType(TypedDict):
    username: str | None
    password: str | None


class MySQLAutoPropertiesType(TypedDict):
    parameters: MySQLAutoParametersType


class MySQLAutoEngineSpec(MySQLEngineSpec, BasicParametersMixin):
    """Custom Engine for MySQL Auto Connection"""

    engine_name = "MySQL Auto"
    engine = "mysql"
    default_driver = "mysqldb"

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
                database="database_try",
                password=parameters.get("password"),
                host="localhost",
                port=3306,
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
    def validate_parameters(cls, properties: MySQLAutoPropertiesType) -> list[SupersetError]:
        """
        Validates parameters for a MySQL connection.
        """
        errors: list[SupersetError] = []

        parameters = properties.get("parameters", {})
        required_params = {"password", "username"}

        missing_params = required_params - set(parameters.keys())
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

        # Attempting to connect to MySQL
        try:
            # Assuming MySQL is running locally on default port 3306
            url = "mysql://{}:{}@localhost:3306/database_test".format(username, password)
            # Attempt to establish a connection
            # engine = create_engine(url)
            # engine.connect()
        except Exception as e:
            errors.append(
                SupersetError(
                    message=f"Error connecting to MySQL database: {str(e)}",
                    error_type=SupersetErrorType.CONNECTION_ACCESS_DENIED_ERROR,
                    level=ErrorLevel.ERROR,
                )
            )

        return errors

