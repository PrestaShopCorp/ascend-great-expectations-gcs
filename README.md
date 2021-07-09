# Ascend.io / Great Expectations docker image for Google Cloud storage

This image is a wrapper around official Ascend.io image to use Great Expectations validation tool.

## Build the docker image

The image is built with Github action located in this file :  .github/workflows/docker-build.yaml.

For now the image is pushed on docker hub at this address: fosk06/ascend-great-expectations-gcs:latest

The image is build on push on main branch and with git tag with the following form "v{X}.{Y}.{Z}"

With:

- X = Major version
- Y = Minor version
- Z = Correction version
