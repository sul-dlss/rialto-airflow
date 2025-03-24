# frozen_string_literal: true

set :application, 'rialto-airflow'
set :repo_url, 'https://github.com/sul-dlss-labs/rialto-airflow.git'

# Default branch is :master
ask :branch, `git rev-parse --abbrev-ref HEAD`.chomp

# Default deploy_to directory is /var/www/my_app_name
set :deploy_to, "/opt/app/rialto/#{fetch(:application)}"

# Default value for :format is :airbrussh.
# set :format, :airbrussh

# You can configure the Airbrussh format using :format_options.
# These are the defaults.
# set :format_options, command_output: true, log_file: "log/capistrano.log", color: :auto, truncate: :auto

# Default value for :log_level is :debug
set :log_level, :info

# Default value for :pty is false
# set :pty, true

# Only using capistrano for docker compose based deployment of a python app, these aren't currentl used.
# set :linked_files, %w[config/honeybadger.yml]
# set :linked_dirs, %w[log config/settings public/system]
# set :dereference_dirs, %w[config/settings]

# Default value for default_env is {}
# set :default_env, { path: "/opt/ruby/bin:$PATH" }

# Default value for local_user is ENV['USER']
# set :local_user, -> { `git config user.name`.chomp }

# Default value for keep_releases is 5
# set :keep_releases, 5

# Uncomment the following to require manually verifying the host key before first deploy.
# set :ssh_options, verify_host_key: :secure

# honeybadger_env otherwise defaults to rails_env
set :honeybadger_env, fetch(:stage)

# Set Rails env to production in all Cap environments
set :rails_env, 'production'

set :docker_compose_file, 'compose.prod.yaml'
set :docker_compose_migrate_use_hooks, false
set :docker_compose_seed_use_hooks, false
set :docker_compose_rabbitmq_use_hooks, false
set :docker_compose_build_use_hooks, false
set :docker_compose_restart_use_hooks, true
set :docker_compose_copy_assets_use_hooks, false
set :honeybadger_use_hooks, false
