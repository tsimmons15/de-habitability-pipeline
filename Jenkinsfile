pipeline {
    agent any
    
    stages {
        stage('Upgrade pip') {
            steps {
                sh '''
                    python3.12 -m pip install --upgrade pip setuptools wheel
                '''
            }
        }
        
        stage('1. Setup venv') {
            steps {
                sh '''
                    python3.12 -m venv .venv
		    source .venv/bin/activate
		    python -m pip install -r requirements.txt
		    echo "${WORKSPACE}"
		    which python
                '''
		script {
		    env.PYTHON = "${env.WORKSPACE}/.venv/bin/python"
		    env.SPARK_SUBMIT = "${env.WORKSPACE}/.venv/bin/spark-submit"
		}
		
            }
        }
    	stage('2. Verify pip install') {
    	    steps {
        		sh '''
			    alias python='${PYTHON}'
                            python --version
			    which python
        		    python -m pip list
        		'''
    	    }
    	}

        stage('3. Data Integration (Postgres Load)') {
            steps {
                echo "Running Data Integration — Loading API data into Postgres..."
                //sh '''
                //   python --version
		//   which python
                //   python -m main ingestion
                //'''
            }
        }
        
        stage('4. Run Silver Transformations') {
            steps {
                echo "Running Silver layer Spark transformations..."
                sh '''
                    #python -m main cleaning
                '''
            }
        }
       
        stage('5. Create Gold Table (Hive)') {
            steps {
                sh '''
                    #python -m main transformation
                '''
            }
        }
    }
    post {
        // This block runs after the entire pipeline finishes
        always {
            deleteDir() /* Deletes the workspace content */
        }
    }
}
