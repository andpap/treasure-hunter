FROM clojure:openjdk-17-tools-deps-alpine

RUN apk add curl

COPY . /app
WORKDIR /app

RUN clojure

HEALTHCHECK --interval=1s --timeout=1s --retries=10 CMD curl http://${ADDRESS}:8000/balance || exit 1

ENTRYPOINT ["clojure", "-X", "core/main"]