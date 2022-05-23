# dataficator

[![etl](https://github.com/usds/dataficator/actions/workflows/etl.yml/badge.svg)](https://github.com/usds/dataficator/actions/workflows/etl.yml)

This repo contains sample code for reading data from a Cloudflare R2 bucket, applying some transformation to the data, and writing the results back to a Cloudflare R2 bucket. All of those operations are triggered by a Github Actions workflow associated with the repository, and triggered on a scheduled basis (the Github Actions equivalent of a cron job).

