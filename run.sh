#!/usr/bin/env bash
uwsgi --plugins http,python --http :8080 --module wsgi --callable app --workers 4