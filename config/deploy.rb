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
set :docker_compose_build_use_hooks, true
set :docker_compose_restart_use_hooks, true
set :docker_compose_copy_assets_use_hooks, false
set :docker_prune_use_hooks, true
set :honeybadger_use_hooks, false

set :alembic_dbs, ['rialto_reports']

def alembic_dbs
  ENV['ALEMBIC_DBS']&.gsub(/\s/, '')&.split(',') || fetch(:alembic_dbs)
end

namespace :python do
  desc 'Install uv package manager (to ~/.local/bin/uv)'
  task :install_uv do
    on roles(:'/.*/') do
      execute 'pip3 install --upgrade uv > uv-installation.log'
    end
  end
end

namespace :db do
  desc 'Create needed databases'
  task :create do
    alembic_dbs.each do |database|
      on roles(:dbmigrate) do
        execute "cd #{release_path} && ~/.local/bin/uv run python rialto_airflow/schema/bin/create_databases.py"
      end
    end
  end
end

# TODO: fix these migrations!  there are currently two issues: they aren't invoked automatically, and they
# raise an authN error when they are invoked manually.  This is probably where the authN error on manual invocation will
# be fixed.  see https://github.com/sul-dlss/rialto-airflow/issues/548 and https://github.com/sul-dlss/rialto-airflow/issues/547
namespace :alembic do
  # TODO: the -name flag is currently ignored, and was somewhat prematurely added because it was in use in libsys-airflow,
  # which provided an example for this Alembic work.  See https://github.com/sul-dlss/rialto-airflow/issues/549
  desc 'Run Alembic database migrations'
  task :migrate do
    alembic_dbs.each do |database|
      on roles(:dbmigrate) do
        execute "cd #{release_path} && ~/.local/bin/uv run alembic --name #{database} upgrade head"
      end
    end
  end

  desc 'Show current Alembic database migration'
  task :current do
    alembic_dbs.each do |database|
      on roles(:dbmigrate) do
        execute "cd #{release_path} && echo #{database.upcase} && ~/.local/bin/uv run alembic --name #{database} current"
      end
    end
  end

  desc 'Show Alembic database migration history'
  task :history do
    alembic_dbs.each do |database|
      on roles(:dbmigrate) do
        execute "cd #{release_path} && ~/.local/bin/uv run alembic --name #{database} history --verbose"
      end
    end
  end
end

# TODO: fix these migrations!  there are currently two issues: they aren't invoked automatically, and they
# raise an authN error when they are invoked manually.  This is probably where the automatic invocation issue will
# be fixed. see https://github.com/sul-dlss/rialto-airflow/issues/548 and https://github.com/sul-dlss/rialto-airflow/issues/547
before 'db:create', 'python:install_uv'
before 'alembic:migrate', 'python:install_uv'
before 'alembic:current', 'python:install_uv'
before 'alembic:history', 'python:install_uv'

on roles(:dbmigrate) do
  before 'docker_compose:up' do
    invoke 'db:create'
    invoke 'alembic:migrate'
  end
end
