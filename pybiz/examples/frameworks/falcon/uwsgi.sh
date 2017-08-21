uwsgi \
    --http :9000 \
    --wsgi-file main.py \
    --virtualenv "$VIRTUAL_ENV" \
    --callable api
