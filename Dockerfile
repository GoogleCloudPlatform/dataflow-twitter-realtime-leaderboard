FROM gcr.io/dataflow-templates-base/python3-template-launcher-base

#ARG WORKDIR=/dataflow/template
#RUN mkdir -p ${WORKDIR}
#WORKDIR ${WORKDIR}


# Do not include `apache-beam` in requirements.txt
ENV FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE="/template/requirements.txt"
ENV FLEX_TEMPLATE_PYTHON_PY_FILE="/template/tweet-pipeline.py"

COPY ./src/ /template

RUN apt-get update \
        && apt-get install -y libffi-dev git \
        && rm -rf /var/lib/apt/lists/* \
        && pip install --no-cache-dir --upgrade pip \
        && pip install --no-cache-dir -r $FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE \
        && pip download --no-cache-dir --dest /tmp/dataflow-requirements-cache -r $FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE

ENV PIP_NO_DEPS=True

# Install apache-beam and other dependencies to launch the pipeline
#RUN pip install apache-beam[gcp]
#RUN pip install -U -r ./requirements.txt
