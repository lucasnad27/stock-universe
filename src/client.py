"""Provides access to td ameritade client."""
import json
import logging
import os

import boto3
import tda

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class TdClient(tda.client.synchronous.Client):
    """
    Provides a td ameritrade client for the user.

    Uses locally stored td_token.json to authenticate and provide a client. If one does not exist,
    instructs user to authenticate through tdameritrade and persists a json file for future use
    """

    _instance = None

    def __new__(cls):
        """Provides a nice singleton wrapper for the client."""
        if cls._instance is None:
            cls._instance = cls._get_client()
        return cls._instance

    @classmethod
    def _get_client(cls) -> tda.client.synchronous.Client:
        """Pulls token from AWS Secrets manager and returns a json token object"""
        secret_name = os.environ["TD_TOKEN_SECRET_NAME"]
        client = boto3.client("secretsmanager")
        api_key = os.environ["TD_API_KEY"]

        def _get_token_from_secrets_manager():
            """Gets token from secrets manager"""
            response = client.get_secret_value(SecretId=secret_name)
            return json.loads(response["SecretString"])

        def _noop_token_write(*ars, **kwargs):
            pass

        client = tda.auth.client_from_access_functions(
            token_read_func=_get_token_from_secrets_manager, token_write_func=_noop_token_write, api_key=api_key
        )
        return client
