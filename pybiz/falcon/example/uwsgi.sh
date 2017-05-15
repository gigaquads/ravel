uwsgi \
    --http :9000 \
    --wsgi-file business.py \
    --virtualenv "$VIRTUAL_ENV" \
    --callable api
