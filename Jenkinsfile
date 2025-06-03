pipeline {
    agent any
    environment {
        DAG_DEPLOY_PATH = "/home/user/airflow/dags"
    }

    stages {
        stage('Run flake8') {
            steps {
                sh '''                // 쉘 스크립트 실행 (멀티라인 사용 시 ''' ''' 사용)
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
                sh 'cp -r python/dags/ ${DAG_DEPLOY_PATH}'
            }
        }
    }
}
