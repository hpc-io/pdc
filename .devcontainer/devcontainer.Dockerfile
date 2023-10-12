# Note: Run `docker build -f .devcontainer/Dockerfile -t pdc:latest .` from the root directory of the repository to build the docker image.

# Use Ubuntu Jammy (latest LTS) as the base image
FROM zhangwei217245/pdc_dev_base:latest

RUN mkdir -p /home/codespace
RUN mkdir -p /home/codespace/source