# frozen_string_literal: true

# Roles are passed to docker-compose as profiles.
server 'sul-rialto-airflow-stage.stanford.edu', user: 'rialto', roles: %w[app]

Capistrano::OneTimeKey.generate_one_time_key!
