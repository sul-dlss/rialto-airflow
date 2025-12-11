# rialto-airflow

[![.github/workflows/test.yml](https://github.com/sul-dlss-labs/rialto-airflow/actions/workflows/test.yml/badge.svg)](https://github.com/sul-dlss-labs/rialto-airflow/actions/workflows/test.yml)
[![codecov](https://codecov.io/gh/sul-dlss/rialto-airflow/graph/badge.svg?token=rmO5oWki9D)](https://codecov.io/gh/sul-dlss/rialto-airflow)

This repository contains an Airflow setup for harvesting and analyzing Stanford publication metadata. The workflow integrates data from [sul_pub](https://github.com/sul-dlss/sul_pub), [rialto-orgs](https://github.com/sul-dlss/rialto-orgs), [OpenAlex](https://openalex.org/), [Dimensions](https://www.dimensions.ai/), [Web of Science](https://clarivate.com/academia-government/scientific-and-academic-research/research-discovery-and-referencing/web-of-science/), [Crossref](https://crossref.org) APIs to provide a view of publication data for Stanford University research.

## Running Locally with Docker

Based on the documentation, [Running Airflow in Docker](https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html).

1. Clone repository `git clone git@github.com:sul-dlss/rialto-airflow.git` (cloning using the git over ssh URL will make it easier to push changes back than using the https URL)

2. Create a `.env` file with the following values. For local development these can usually be as below.  To ensure the tests work correctly, look at the section below labeled `Test Setup`, which will tell you exactly which values need to be set and from where in order to have the tests work.
```
AIRFLOW_UID=50000
AIRFLOW_GROUP=0
AIRFLOW_VAR_DATA_DIR="data"
AIRFLOW_VAR_OPENALEX_EMAIL=rialto-service@lists.stanford.edu
AIRFLOW_VAR_SUL_PUB_HOST: 'sul-pub-cap-uat.stanford.edu'
AIRFLOW_VAR_HARVEST_LIMIT: 1000
AIRFLOW_VAR_WOS_KEY: see vault value at puppet/application/rialto-airflow/dev/wos_key
```
(See [Airflow docs](https://airflow.apache.org/docs/apache-airflow/2.9.2/howto/docker-compose/index.html#setting-the-right-airflow-user) for more info.)

3. Add environment variables used by DAGs to the `.env` file. For the VMs, they will be applied by puppet.  For localhost, you can use the following to generate secret content for your dev .env file from stage (you can also use prod if you really needed to by altering where in puppet you look below).  For running tests, you may need to add some values as described below.  Note that you should use the WOS key from the dev environment as described above, not the stage environment which will be output below.

```
for i in `vault kv list -format yaml puppet/application/rialto-airflow/stage | sed 's/- //'` ; do \
  val=$(echo $i| tr '[a-z]' '[A-Z]'); \
  echo AIRFLOW_VAR_$val=`vault kv get -field=content puppet/application/rialto-airflow/stage/$i`; \
done
```

4. The harvest DAG requires a CSV file of authors (authors.csv) from rialto-orgs to be available on the filesystem. A version of this file is available on the rialto-orgs VM at `/rialto-data/authors.csv` or at https://sul-rialto-stage.stanford.edu/authors?action=index&commit=Search&controller=authors&format=csv&orcid_filter=&q=&only_active=false.

The publish_orcid_to_reports DAG requires a CSV of active authors (active_authors.csv). A version of this file is available on the rialto-orgs VM at `/rialto-data/active_authors.csv` or at https://sul-rialto-stage.stanford.edu/authors?action=index&commit=Search&controller=authors&format=csv&orcid_filter=&q=

Put the `authors.csv` and `authors_active.csv` file in the `data/` directory.

5. The publish_to_reports DAG expects a `downloads/` directory to exist in the directory for `AIRFLOW_VAR_DATA_DIR`.

6. Bring up containers.
```
docker compose up -d
```

7. The Airflow application will be available at `localhost:8080` and can be accessed with the default Airflow username and password: "airflow" and "airflow".

## Development

### Console

```
uv run dotenv run python
```

Testing database queries on the console after starting up python.

Note: you may need to specify the  `AIRFLOW_VAR_RIALTO_POSTGRES` connection string in your `.env` file like this:

`AIRFLOW_VAR_RIALTO_POSTGRES="postgresql+psycopg2://airflow:airflow@localhost:5432"`

```
from rialto_airflow.snapshot import Snapshot
from sqlalchemy import select, func
from rialto_airflow.database import get_session, Publication, Author, Funder

# the database name
database_name = 'rialto_20250508215121'

snapshot = Snapshot('data',database_name=database_name)
session = get_session(snapshot.database_name)()

# query an entire table
stmt = (select(Publication).limit(10))
# stmt = (select(Publication).where(Publication.wos_json.isnot(None)).limit(10))
result = session.execute(stmt)

for row in result:
    print(f"DOI: {row.Publication.doi}")

# note: once you iterate over the result set once, you will need to re-execute the query
# if you want to access it again like this:

row = result.first()
row.Publication.doi
row.Publication.dim_json
row.Publication.dim_json['journal']['title']

# query specific columns, leave off the table name when inspecting results
stmt = (select(Publication.doi, Publication.dim_json, Publication.openalex_json, Publication.wos_json, Publication.sulpub_json).limit(10))
result = session.execute(stmt)

row = result.first()
row.doi
row.dim_json
```

### Set-up

1. Install `uv` for dependency management as described in [the uv docs](https://github.com/astral-sh/uv?tab=readme-ov-file#getting-started).  _NOTE:_ As of Feb 2025, at least one developer has had better luck with dependency management using the `uv` standalone installer, as opposed to installing using `pip` or `pipx`.  YMMV of course, but if you run into hard to explain `pyproject.toml` complaints or dependency resolution issues, consider uninstalling the `pip` managed `uv`, and installing from the `uv` installation script.

To add a dependency, e.g. flask:
1. `uv add flask`
3. Then commit `pyproject.toml` and `uv.lock` files.

You'll have to rebuild and restart containers for new dependencies to get picked up (both locally and in deployed envs), since the dependency installations are baked into the container image at build time (unlike the DAG/task code). Like our Rails apps, a redeploy should take care of rebuild and restart in deployed environments.

### Upgrading dependencies
To upgrade Python dependencies:
```
uv lock --upgrade
```

Like dependency additions, dependency updates require container rebuild/restart.

## Database Management (Creation and Migration)
As of Sept 2025, we have 3 types of database:
* The internal Airflow DB, which the Airflow Docker containers manage.
* Harvest snapshot DBs, one of which is created on each harvest run.
* Other persistent databases with stable names, e.g. the `rialto_reports` DB to which Tableau connects to render dashboard visualizations.

We use [SQLAlchemy](https://www.sqlalchemy.org/) as our ORM, and [Alembic](https://alembic.sqlalchemy.org/en/latest/) to manage migrations as our schema(s) evolve.

For migrations to run properly, the PostgreSQL database must be running and available.  In local dev and in CI, this will be a Docker container.  In stage and prod, it will be a standalone PG VM, as is typical of our other app setups.

For local (laptop) dev and for deployed environments (stage and prod), database migrations should automatically run as one of the services defined by the Docker `compose.yaml` and `compose.prod.yaml` files.  Since CI doesn't spin up the full Docker stack, just the `postgres` container, the migration commands are run as a CI step.

If you need to manually run migrations or related Alembic commands in a deployed environment, you can do one of the following from an SSH session on the VM:
```sh
# in stage and prod, make sure that uv is installed, and is up to date, as it's needed for all of the other commands
pip3 install --upgrade uv # in stage and prod, will install to ~/.local/bin/uv (which should be in $PATH by default)

# run the following from the rialto-airflow project directory (the git project dir on localhost, or ~/rialto-airflow/current in deployed envs)

# create the database(s) for which Alembic manages migrations -- this must happen before migrations run
uv run python bin/create_databases.py

uv run dotenv alembic upgrade head # run the migrations to get to the current DB schema for the deployment
uv run dotenv alembic current # show the current migration revision for this env
uv run dotenv alembic history --verbose # show the history of migrations that have been run in this env
```

## Run Tests
To run the tests, you'll need to be up to date on database migrations in your local dev environment.  That should happen automatically if you've started the full Docker stack since the last migration was added, and not wiped the `postgres` volume since that.  If you've not done that, and you just want to manually run the migrations after starting the `postgres` container, see the Database Management section above.

Once you're up to date on migrations, you can run:
```sh
docker compose up -d postgres
uv run pytest
```

### Test Setup

In order for some of the tests to run, they will need to hit actual APIs.  In order to do this,
they will need to be properly configured with keys and URLs.  These need to be placed in the .env file.

Note that if you have hard-coded values in the compose.yml files, they will override any hardcoded values in the .env file.

For MaIS tests, update your .env with values shown below / pulled from vault as indicated:

```
AIRFLOW_VAR_MAIS_TOKEN_URL=https://mais-uat.auth.us-west-2.amazoncognito.com
AIRFLOW_VAR_MAIS_BASE_URL=https://mais.suapiuat.stanford.edu
AIRFLOW_VAR_MAIS_CLIENT_ID=${get from vault at `puppet/application/rialto-airflow/stage/mais_client_id`}
AIRFLOW_VAR_MAIS_SECRET=${get from vault at `puppet/application/rialto-airflow/stage/mais_secret`}
```

Note: The MaIS `test_mais.py` file depends on the MaIS API being configured specifically with the UAT (not prod) credentials.  If no credentials are available in the environment variables, the tests will be skipped completely.  If production credentials are supplied, some of the tests may fail, since they assert checks against UAT data.

### Test coverage reporting

In addition to the terminal display of a summary of the test coverage percentages, you can get a detailed look at which lines are covered or not by opening `htmlcov/index.html` after running the test suite.

### Linting and formatting

1. Run linting: `uv run ruff check`
2. Automatically fix lint: `uv run ruff check --fix`
3. Run formatting: `uv run ruff format` (or `uv run ruff format --check` to identify any unformatted files,  or `uv run ruff format --diff` to see what would change without applying)

### Type Checking

To see if there are any type mismatches:

```
uv run mypy .
```

### Run all the checks

One line for running the linter, the type checker, and the test suite (failing fast if there are errors):
```
uv run ruff format --diff . && uv run ruff check && uv run mypy . && uv run pytest
```

### Troubleshooting

Getting errors with dependencies when trying to run the tests that look something like this?

`AttributeError: module 'psycopg2' has no attribute 'paramstyle'`

Try clearing the virtual env folder and let it rebuild on the next run: `rm -rf .venv`

## CLI

The `rialto` command line utility lets you run some bespoke data export tasks, like exporting a CSV of publications for a particular author:

```
uv run rialto publications makeller
```

Or there is this mouthful if you want to run it in a running Docker environment:

```
docker compose exec -u airflow -ti airflow-worker uv run rialto publications makeller
```

## Deployment

Deployment to https://sul-rialto-airflow-XXXX.stanford.edu/ is handled like other SDR services using Capistrano. You'll need to have Ruby installed and then:

```
bundle exec cap stage deploy # stage
bundle exec cap prod deploy  # prod
# Note: there is no QA
```

By default, deployment will use the deployed branch's code to build a fresh Docker image locally, and will restart `docker compose`, which will cause it to pick up the new image.

If you'd like to push an image to Docker Hub (or any other registry) for deployment, consider reserving the `latest` tag for builds from the `main` branch, and pushing a branch-specific image tag if deploying a branch other than `main`.  See `compose.yaml` (for local dev) and `compose.prod.yaml` (for stage/prod deployment) for more detail on using the registry image instead of a build from `Dockerfile`.


### API Keys

In order to run harvests locally and in stage, you will need actual API keys for the various services in your local .env file.  In some cases, the same API key can be used for all environments, but in other cases where there are usage limits, we will need to use different API keys for dev, stage and/or prod.

The setup section above describes how to obtain API keys from vault.  For local development, you will generally use the ones indicated in the stage vault environment.

The following API keys are actually the same in all environments because they are pulling publication data and we do not believe the usage limits will be exceeded:

- Dimensions
- OpenAlex
- Pubmed
- Mais ORCID (note there is a UAT key available, but it will return limited test data only and so is not useful for us)

The following API keys should be different in each environment (i.e. local dev has a different key than stage and a different key than prod), even though they are all pulling from the same data, in order to better isolate usage and avoid hitting usage limits:

- WoS
