# SSS Healthcheck

Internal service endpoint health check for the Spatial Support System.

# Installation

The recommended way to set up this project for development is using
[Poetry](https://python-poetry.org/docs/) to install and manage a virtual Python
environment. With Poetry installed, change into the project directory and run:

    poetry install

To run Python commands in the virtualenv, thereafter run them like so:

    poetry run python

Manage new or updating project dependencies with Poetry also, like so:

    poetry add newpackage==1.0

# Environment variables

This project uses **python-dotenv** to set environment variables (in a `.env` file).
Most settings have default values; check `status.py` for required variables.

# Running

Use `python status.py` to run a local copy of the application:

    poetry run python status.py

The application runs on port 8080 by default. To change this, set an environment
variable value for `PORT`.

# Docker image

To build a new Docker image from the `Dockerfile`:

    docker image build -t ghcr.io/dbca-wa/healthcheck .

To run a Docker container locally, publishing container port 8080 to a local port:

    docker container run --rm --publish 8080:8080 --env-file .env ghcr.io/dbca-wa/healthcheck
