A lightweight and fast api for interacting with odm2

Have a look at [ODM2 entity documentation](https://github.com/ODM2/ODM2/wiki/documentation#odm2core-entities) for a description of the datamodel.
## Development

### Requirements

- docker
- docker-compose
- python 3.8

### Initial setup

Start all services:

```
docker-compose build
docker-compose up

# alternatively, for running in background
docker-compose up -d
```

After a while, you should have all services running:

```
docker-compose ps

                Name                               Command               State             Ports          
----------------------------------------------------------------------------------------------------------
odm2-postgres-api_db_init_1             python src/odm2_postgres_a ...   Exit 0                           
odm2-postgres-api_graphql_1             ./cli.js --enhance-graphiq ...   Up       127.0.0.1:8702->5000/tcp
odm2-postgres-api_odm2_postgres_api_1   python /app/src/odm2_postg ...   Up       127.0.0.1:8701->5000/tcp
odm2-postgres-api_timescale_odm2_1      docker-entrypoint.sh postgres    Up       0.0.0.0:8700->5432/tcp  
```

odm2_postgres_api initialize some metadata on startup. First time setting up this might take some seconds before all metadata is populated.

## Controlled vocabularies

ODM2 relies on community-defined vocabularies, further explained at http://vocabulary.odm2.org/. In order to streamline environment setup we have downloaded a local copy of vocabularies at [controlled_vocabularies](./src/odm2_postgres_api/controlled_vocabularies/cv_definitions).

These are manually downloaded using the following script:

```
python src/odm2_postgres_api/controlled_vocabularies/download_cvs.py
```

This will download vocabularies, and if there are any new definitions, check these into git. This way the controlled vocabularies will be automatically be populated in all environments.

#### Troubleshooting

- how do I access the postgres database?

postgres runs on 127.0.0.1:8700, username = niva_odm2_read_only_user; db = niva_odm2
