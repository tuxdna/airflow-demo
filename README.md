Follow the steps here:

 * https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html

Verify you have enough RAM for docker setup

```
% docker run --rm "debian:bookworm-slim" bash -c 'numfmt --to iec $(echo $(($(getconf _PHYS_PAGES) * $(getconf PAGE_SIZE))))'
...
6a2d07df495c: Pull complete
...
7.7G
```

Fetch docker compose file for this project setup

```
% curl -LfO 'https://airflow.apache.org/docs/apache-airflow/3.2.1/docker-compose.yaml'
```

Create required directories, and config

```
mkdir -p ./dags ./logs ./plugins ./config
echo -e "AIRFLOW_UID=$(id -u)" > .env
docker compose up airflow-init
```

Edit .env file to add additional requirement
```
_PIP_ADDITIONAL_REQUIREMENTS=dag-factory>=1.1.0
```

Start airflow setup

```
docker compose up -d
```

Check the status

```
docker compose run airflow-worker airflow info
```

Now go to http://localhost:8080 with credentials: airflow/airflow 

Run the example DAG

```
./airflow.sh dags trigger airflow_capability_demo
```

