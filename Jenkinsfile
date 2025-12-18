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
			    source .venv/bin/activate
        		    python -m pip list
        		'''
    	    }
    	}

	stage('3. Setup Jenkins file system, mainly log and csv dirs.') {
	    steps {
		script {
		    mkdir "${params.jenkins_log_dir}"
		    mkdir "${params.csv_dir}"
		}
	    }
	}
        stage('4. Data Integration (Postgres Load)') {
            steps {
                echo "Running Data Integration — Loading API data into Postgres..."
                sh '''
		   source .venv/bin/activate
                   python -m main ingestion
                '''
            }
        }
        stage('5. Run Silver Transformations') {
            steps {
                echo "Running Silver layer Spark transformations..."
                sh '''
                    source .venv/bin/activate
		    #python -m main cleaning
                '''
            }
        }
       
        stage('6. Create Gold Table (Hive)') {
            steps {
                sh '''
                    source .venv/bin/activate
		    #python -m main transformation
                '''
            }
        }
    }
    post {
        // This block runs after the entire pipeline finishes
        always {
            deleteDir() /* Deletes the workspace content */
	    //HDFS upload of log files to log_dir.
        }
    }
}
