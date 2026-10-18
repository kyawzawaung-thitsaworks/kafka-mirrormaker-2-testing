"""Optional login for the web UIs (HTTP basic authentication).

Set UI_PASSWORD in lab.env to turn it on. The browser then asks for a user
name (UI_USER, default "lab") and the password once per session.
"""
import hmac
import os

from flask import Response, request

UI_USER = os.environ.get("UI_USER", "lab")
UI_PASSWORD = os.environ.get("UI_PASSWORD", "")


def protect(app):
    if not UI_PASSWORD:
        return

    @app.before_request
    def _require_login():
        auth = request.authorization
        if (
            auth
            and hmac.compare_digest(auth.username or "", UI_USER)
            and hmac.compare_digest(auth.password or "", UI_PASSWORD)
        ):
            return None
        return Response(
            "Log in with the user name and password from lab.env.\n",
            401,
            {"WWW-Authenticate": 'Basic realm="Kafka DR lab"'},
        )
