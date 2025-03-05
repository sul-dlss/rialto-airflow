# rialto-airflow

[![.github/workflows/test.yml](https://github.com/sul-dlss-labs/rialto-airflow/actions/workflows/test.yml/badge.svg)](https://github.com/sul-dlss-labs/rialto-airflow/actions/workflows/test.yml)
[![codecov](https://codecov.io/gh/sul-dlss/rialto-airflow/graph/badge.svg?token=rmO5oWki9D)](https://codecov.io/gh/sul-dlss/rialto-airflow)

Airflow for harvesting data for open access analysis and research intelligence. The workflow integrates data from [sul_pub](https://github.com/sul-dlss/sul_pub), [rialto-orgs](https://github.com/sul-dlss/rialto-orgs), [OpenAlex](https://openalex.org/) and [Dimensions](https://www.dimensions.ai/) APIs to provide a view of publication data for Stanford University research. The basic workflow is: fetch Stanford Research publications from SUL-Pub, OpenAlex, and Dimensions, enrich them with additional metadata from OpenAlex and Dimensions using the DOI, merge the organizational data found in [rialto_orgs], and publish the data to our JupyterHub environment.

```mermaid

flowchart TB
  
  subgraph Harvest-by-ORCID
    direction RL
    Dimensions-by-ORCID
    OpenAlex-by-ORCID
    PubMed-by-ORCID
    WebOfScience-by-ORCID
  end
  
  subgraph Harvest-by-DOI
    direction RL
    Dimensions-by-DOI
    OpenAlex-by-DOI
    PubMed-by-DOI
    WebOfScience-by-DOI
  end
  
  subgraph Publish
    Data-Quality
    Open-Access
    Stanford
    ORCID-Adoption
    Publisher-Contracts
  end
  
  Setup --> Load-Authors
  
  Load-Authors --> Harvest-sulpub
   
  Load-Authors --> Harvest-by-ORCID
  
  Harvest-sulpub --> Harvest-by-DOI
  
  Harvest-by-ORCID --> Harvest-by-DOI
  
  Harvest-by-DOI --> Distill
  
  Distill --> Publish
  
```

## Running Locally with Docker

Based on the documentation, [Running Airflow in Docker](https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html).

1. Clone repository `git clone git@github.com:sul-dlss/rialto-airflow.git` (cloning using the git over ssh URL will make it easier to push changes back than using the https URL)

2. Start up docker locally.

3. Create a `.env` file with the `AIRFLOW_UID` and `AIRFLOW_GROUP` values. For local development these can usually be:
```
AIRFLOW_UID=50000
AIRFLOW_GROUP=0
AIRFLOW_VAR_DATA_DIR="data"
```
(See [Airflow docs](https://airflow.apache.org/docs/apache-airflow/2.9.2/howto/docker-compose/index.html#setting-the-right-airflow-user) for more info.)

4. Add to the `.env` values for any environment variables used by DAGs. Not in place yet--they will usually applied to VMs by puppet once productionized.

Here is an script to generate content for your dev .env file:

```
for i in `vault kv list -format yaml puppet/application/rialto-airflow/stage | sed 's/- //'` ; do \
  val=$(echo $i| tr '[a-z]' '[A-Z]'); \
  echo AIRFLOW_VAR_$val=`vault kv get -field=content puppet/application/rialto-airflow/stage/$i`; \
done
```

Additionally, you may want a value for `AIRFLOW_VAR_MAIS_BASE_URL`. This is available from the rialto-orgs configuration (either in the [base config](https://github.com/sul-dlss/rialto-orgs/blob/main/config/settings.yml), or overridden via shared_configs).

5. The harvest DAG requires a CSV file of authors from rialto-orgs to be available. This is not yet automatically available, so to set up locally, download the file at
https://sul-rialto-stage.stanford.edu/authors?action=index&commit=Search&controller=authors&format=csv&orcid_filter=&q=. Put the `authors.csv` file in the `data/` directory.

6. Bring up containers.
```
docker compose up -d
```

7. The Airflow application will be available at `localhost:8080` and can be accessed with the default Airflow username and password.

## Development

### Set-up

1. Install `uv` for dependency management as described in [the uv docs](https://github.com/astral-sh/uv?tab=readme-ov-file#getting-started).  _NOTE:_ As of Feb 2025, at least one developer has had better luck with dependency management using the `uv` standalone installer, as opposed to installing using `pip` or `pipx`.  YMMV of course, but if you run into hard to explain `pyproject.toml` complaints or dependency resolution issues, consider uninstalling the `pip` managed `uv`, and installing from the `uv` installation script.

To add a dependency, e.g. flask:
1. `uv add flask`
3. Then commit `pyproject.toml` and `uv.lock` files.

### Upgrading dependencies
To upgrade Python dependencies:
```
uv lock --upgrade
```

## Run Tests

```
docker compose up -d postgres
uv run pytest
```

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

## Deployment

First you'll need to build a Docker image and publish it DockerHub:

```
DOCKER_DEFAULT_PLATFORM="linux/amd64" docker build . -t suldlss/rialto-airflow:latest
docker push suldlss/rialto-airflow
```

Deployment to https://sul-rialto-airflow-XXXX.stanford.edu/ is handled like other SDR services using Capistrano. You'll need to have Ruby installed and then:

```
bundle exec cap stage deploy # stage
bundle exec cap prod deploy  # prod
# Note: there is no QA
```
