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

Writing API is available at http://localhost:8701
Reading GraphQL API is available at http://localhost:8702/graphiql

### Populate data

After all services are running ok, we need to populate with some data.

#### download controlled vocabularies
Go to http://localhost:5000/#/default/patch_controlled_vocabularies_controlled_vocabularies_patch and perform the request. Alternatively, copy paste the following in your terminal:
```
curl -X PATCH "http://localhost:5000/controlled_vocabularies" -H "accept: application/json"
```

#### Add metadata

```
python src/odm2_postgres_api/utils/populate_with_general_metadata.py

# In addition, other ferrybox can be added:
python src/odm2_postgres_api/utils/populate_with_ferrybox_metadata.py
```

TODO: These scripts are not idempotent and can only be ran once. Causes both duplicates and in some cases unique errors.
