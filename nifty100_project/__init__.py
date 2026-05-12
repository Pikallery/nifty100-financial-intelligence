"""
Package init for nifty100_project.

Importing the Celery app here ensures it is initialised when Django starts,
so that shared_task decorators in all apps are registered correctly.
"""

from .celery import app as celery_app

__all__ = ("celery_app",)
