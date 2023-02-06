FROM python:latest

RUN apt-get install wget
RUN pip install pandas sqlalchemy psycopg2

ENV URL=https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-02.csv.gz

WORKDIR /app
COPY ingest.py ingest.py

ENTRYPOINT ["python", "ingest.py"]
CMD ["--user=root","--password=root","--host=pgdatabase","--port=5432","--db=ny_taxi","--table_name=yellow_taxi_trips", "--url=${URL}"]