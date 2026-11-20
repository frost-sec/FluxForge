ARG PYTHON_IMAGE=python:3.13.7
FROM ${PYTHON_IMAGE}
RUN python -m pip install --no-cache-dir matplotlib==3.10.8
COPY plot.py /usr/local/bin/plot.py
ENV MPLBACKEND=Agg MPLCONFIGDIR=/tmp/matplotlib
ENTRYPOINT ["python", "/usr/local/bin/plot.py"]
