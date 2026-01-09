ARG FUNCTION_DIR="/function"

FROM --platform=linux/arm64 python:3.11

# Include global arg in this stage of the build
ARG FUNCTION_DIR

# Copy function code
RUN mkdir -p ${FUNCTION_DIR}
COPY . ${FUNCTION_DIR}

# Install the function's dependencies
RUN pip install \
    --target ${FUNCTION_DIR} \
        awslambdaric

# setup ssh client to install private git repos
RUN apt-get update -y && apt-get install -y git && apt-get install -y openssh-client
RUN mkdir -p -m 0600 ~/.ssh && \
    ssh-keyscan -H github.com >> ~/.ssh/known_hosts

# Include global arg in this stage of the build
ARG FUNCTION_DIR

WORKDIR ${FUNCTION_DIR}

# Copies requirements.txt file into the container
COPY requirements.txt ${FUNCTION_DIR}

# Installs dependencies found in your requirements.txt file
#TODO be sure to install correct version of ingestlib
RUN --mount=type=ssh pip install git+ssh://git@github.com/synoptic/synoptic-logging.git@v1.0.5
RUN --mount=type=ssh pip install git+ssh://git@github.com/synoptic/ingestlib.git@v5.1.0#egg=ingestlib
RUN pip install -r requirements.txt

# Set runtime interface client as default command for the container runtime
ENTRYPOINT [ "/usr/local/bin/python", "-m", "awslambdaric" ]

# Points to the handler function of your lambda function
CMD ["obs_lambda_handler.main"]