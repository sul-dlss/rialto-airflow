# frozen_string_literal: true

# Roles are passed to docker-compose as profiles.
server 'sul-rialto-airflow-stage.stanford.edu', user: 'rialto', roles: %w[app dbmigrate]
