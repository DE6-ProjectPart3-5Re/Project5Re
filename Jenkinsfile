pipeline {
    agent any
    stages {
        stage('Run flake8') {
            steps {
                sh '''
                    python3 -m venv venv
                    source venv/bin/activate
                    pip install --upgrade pip
                    pip install -r requirements.txt
                    flake8 python/dags/
                    deactivate
                    rm -rf venv
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
