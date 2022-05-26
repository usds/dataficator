# dataficator

[![etl](https://github.com/usds/dataficator/actions/workflows/etl.yml/badge.svg)](https://github.com/usds/dataficator/actions/workflows/etl.yml)

This repo contains sample code for reading data from a Cloudflare R2 bucket, applying some transformation to the data, and writing the results back to a Cloudflare R2 bucket. All of those operations are triggered by a Github Actions workflow associated with the repository, and triggered on a scheduled basis (the Github Actions equivalent of a cron job).

## How to run this manually

The entrypoint for this system is [etl.py](./etl.py).
There is a Github Actions workflow defined in [.github/workflows/etl.yml](./.github/workflows/etl.yml) which specifies an environment and triggers for running etl.py.
Two triggers are defined:
- A cron job (see the yaml file for current intervals)
- A mechanism allowing manual invocation

To manually invoke etl.py inside a Github Action, click the "Actions" tab in Github; click "etl" on the left, and then "Run workflow"

![howto](img/actions.png)

## How to set up Cloudflare R2 buckets

1. Get a Cloudflare account.
2. Log into the [Cloudflare dashboard](https://dash.cloudflare.com)
3. Click on "Workers" in the left-hand side nav menu and enable a Workers subdomain.
4. Click on R2 in the left-hand side nav menu and create two R2 buckets. The etl.py code assumes that they'll be called `incoming` (for inbound data) and `inventory` (for processed results).
5. Make the `inventory` bucket publicly readable by setting up a Cloudflare Worker to intercept incoming requests.
