# airflow docker compose 가이드

### 반드시 다음 볼륨에 대한 경로를 확인해 주어야 합니다.
#### (기본값은 docker compose 파일이 있는 경로로 설정되어 있음)

- ${AIRFLOW_PROJ_DIR:-.}/dags:/opt/airflow/dags #dag volume
- ${AIRFLOW_PROJ_DIR:-.}/logs:/opt/airflow/logs #log volume
- ${AIRFLOW_PROJ_DIR:-.}/config:/opt/airflow/config #config file volume
- ${AIRFLOW_PROJ_DIR:-.}/plugins:/opt/airflow/plugins # plugins volume
- ${AIRFLOW_PROJ_DIR:-.}/pgdata:/var/lib/postgresql/data #metadata를 위한 postgresql volume

##### 만약 docker compose up 실행시 오류가 발생하거나 DB 문제가 발생한다면 아래 명령어를 먼저 실행하여 database를 초기화 하십시오
 > docker compose up airflow-init