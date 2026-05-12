web: gunicorn nifty100_project.wsgi --bind 0.0.0.0:8000 --workers 2
worker: celery -A nifty100_project worker --loglevel=info
beat: celery -A nifty100_project beat --loglevel=info
