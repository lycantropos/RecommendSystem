# Vizier
**Vizier** provides next facilities:
- movie database,
- movie recommendation system.

## Local development
### Setting up
Build `Docker` image:
```bash
docker-compose build
```

Run `Docker` container:
```bash
docker-compose up
```

Next commands should be executed in another terminal tab.

Store `Postgres` container id:
```bash
POSTGRES_CONTAINER_ID=$(docker-compose ps -q hydra-postgres)
```

There are two available options.
1. Restore `Postgres` database from plain dump:
```bash
cat $PATH_TO_POSTGRES_DUMP | docker exec -i $POSTGRES_CONTAINER_ID psql -U $POSTGRES_USER_NAME $POSTGRES_DB_NAME
```
where 
- `PATH_TO_POSTGRES_DUMP` is a path (absolute or relative) 
to `Postgres` dump stored at host machine (ex. `opt/hydra/dumps/dump.sql`),
- `POSTGRES_USER_NAME` is a `Postgres` user name (ex. `heracles`),
- `POSTGRES_DB_NAME` is a target `Postgres` database name (ex. `hydra`).

2. Restore `Postgres` database from dump created with `pg_dump`:
```bash
docker exec -i $POSTGRES_CONTAINER_ID pg_restore -a -U $POSTGRES_USER_NAME -d $POSTGRES_DB_NAME $PATH_TO_POSTGRES_DUMP
```

### Running
Run `Docker` container:
```bash
docker-compose up
```

### Running for development
Run `Docker` container with `PyCharm` `Python Remote Debug`:
```bash
./set-dockerhost.sh docker-compose up
```
