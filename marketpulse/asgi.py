"""ASGI config for MarketPulse."""

import os

from django.core.asgi import get_asgi_application

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "marketpulse.settings")

application = get_asgi_application()
