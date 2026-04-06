FROM node:21-alpine AS web-builder

WORKDIR /web

ENV NEXT_PUBLIC_API_URL=/api \
    NEXT_PUBLIC_WS_URL=/ws \
    NEXT_TELEMETRY_DISABLED=1 \
    SKIP_ENV_VALIDATION=1

COPY ./web/package*.json ./
RUN npm ci --frozen-lockfile

COPY ./web .
RUN npm run build

FROM python:3.11-alpine AS final

WORKDIR /app

ARG TARGETARCH
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    LD_LIBRARY_PATH=/app/tdlib \
    LANG=C.UTF-8 \
    NGINX_PORT=80

RUN addgroup -S tf && \
    adduser -S -G tf tf && \
    apk add --no-cache nginx wget curl unzip tini su-exec gettext openssl3 libstdc++ gcompat libc6-compat && \
    rm -rf /tmp/* /var/tmp/* && \
    touch /run/nginx.pid && \
    chown -R tf:tf /app /etc/nginx /var/lib/nginx /var/log/nginx /run/nginx.pid

COPY ./pyapi/requirements.txt /tmp/requirements.txt
RUN python -m pip install --no-cache-dir -r /tmp/requirements.txt && \
    rm -f /tmp/requirements.txt

COPY --from=web-builder --chown=tf:tf /web/out /app/web/

COPY --chown=tf:tf ./pyapi /app/pyapi

COPY --chown=tf:tf ./tdlib/linux_$TARGETARCH /app/tdlib
COPY --chown=tf:tf ./entrypoint.sh .
COPY --chown=tf:tf ./nginx.conf.template /etc/nginx/nginx.conf.template

EXPOSE $NGINX_PORT

ENTRYPOINT ["/sbin/tini", "--"]
CMD ["/bin/sh", "./entrypoint.sh"]
