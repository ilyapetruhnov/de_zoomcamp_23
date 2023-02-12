FROM prefecthq/prefect:2.7.7-python3.9

COPY docker-req.txt .

RUN pip install -r docker-req.txt --trusted-host pypi.python.org --no-cache-dir

COPY flows /opt/prefect/flows
RUN mkdir -p /opt/prefect/data