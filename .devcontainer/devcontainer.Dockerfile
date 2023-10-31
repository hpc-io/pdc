# Note: Run `docker build -f .devcontainer/Dockerfile -t pdc:latest .` from the root directory of the repository to build the docker image.

# Use Ubuntu Jammy (latest LTS) as the base image
# FROM ubuntu:jammy
FROM hpcio/pdc-dev-base:latest

RUN rm -rf $PDC_SRC_DIR && \
    rm -rf $PDC_DIR
