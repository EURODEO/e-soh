FROM migrate/migrate:4

COPY data/migrations /migrations

COPY ./run-migrate.sh /

RUN chmod +x /run-migrate.sh

ENTRYPOINT [ "/run-migrate.sh" ]
