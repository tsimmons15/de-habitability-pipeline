pipeline {
    agent any
    
    environment {
        REPROCESS = 'False'
    }

    stages {
        stage('Upgrade pip') {
            steps {
                sh """
                    #Adding ensurepip because this morning (20251218) got an error saying pip is not a recognized module...
		    python3.12 -m ensurepip --upgrade
                    python3.12 -m pip install --upgrade pip setuptools wheel
                """
            }
        }
        
        stage('1. Setup venv') {
            steps {
                sh """
                    python3.12 -m venv .venv
		    source .venv/bin/activate
		    python -m pip install -r requirements.txt
                """
		script {
		    env.PYTHON = "${env.WORKSPACE}/.venv/bin/python"
		    env.SPARK_SUBMIT = "${env.WORKSPACE}/.venv/bin/spark-submit"
		}
		
            }
        }
    	stage('2. Verify pip install') {
    	    steps {
        		sh """
			    source .venv/bin/activate
        		    python -m pip list
        		"""
    	    }
    	}

	stage('3. Setup Jenkins file system, mainly log and csv dirs.') {
	    steps {
		sh """
		    mkdir "$params.jenkins_log_dir"
		    mkdir "$params.csv_dir"
		"""

		withFileParameter('csv_artifacts') {
		    sh """
		        test -f "$csv_artifacts"
		        tar -xvzf "$csv_artifacts" raw_data/
		    """
		   
		}
	    }
	}
        stage('4. Data Integration (Postgres Load)') {
            steps {
                echo "Running Data Integration — Loading API data into Postgres..."
                sh """
		   source .venv/bin/activate
                   python -m main ingestion
                """
            }
        }
        stage('5. Run Silver Transformations') {
            steps {
                echo "Running Silver layer Spark transformations..."
                sh """
                    source .venv/bin/activate
		    #python -m main cleaning
                """
            }
        }
       
        stage('6. Create Gold Table (Hive)') {
            steps {
                sh """
                    source .venv/bin/activate
		    #python -m main transformation
                """
            }
        }
	stage('6. Archive') {
	    steps {
	        sh """
		    tar -cvzf logs/* logs_artifact.tar.gz
		    tar -cvzf raw_data/* csv_artifacts.tar.gz
                """
		archiveArtifacts artifacts: '*.tar.gz', onlyIfSuccessful: true
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
