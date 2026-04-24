# CZMon
CZ monitoring tool repo

## Local UI

With the venv active: `python manage.py runserver` then open http://127.0.0.1:8000/

`DEBUG` defaults to **on** so CSS/JS under `/static/` load. For a production-style run, use `DJANGO_DEBUG=false python manage.py runserver` (you must run `collectstatic` and serve static files separately).
