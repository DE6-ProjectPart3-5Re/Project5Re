pipeline {
    agent any
    stages {
        stage('Run flake8') {
            steps {
                sh '''#!/bin/bash
                    python3 -m venv venv
                    source venv/bin/activate
                    pip install --upgrade pip
                    pip install -r requirements.txt
                    flake8 python/dags/ --count --select=E9,F63,F7,F82 --show-source --statistics
                '''
            }
        }

        stage('DAG Deploy') {
            steps {
                sh '''#!/bin/bash
                    docker cp python/dags airflow-airflow-scheduler-1:/opt/airflow/
                '''
                
            }
        }
    }
}
