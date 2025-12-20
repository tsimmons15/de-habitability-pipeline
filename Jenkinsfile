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
		    mkdir "$params.log_dir"
		    mkdir "$params.csv_dir"
		"""
		script {
		    file_name = params.csv_artifact
		    if (file_name) {
		        sh "tar -xvzf $csv_artifacts raw_data/ "
			env.REPROCESS = True
		    }
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
		    tar -cvzf csv_artifacts.tar.gz "$params.csv_dir"*
		    tar -cvzf logs_artifact.tar.gz "$params.log_dir"*
                """
		archiveArtifacts artifacts: '*.tar.gz'
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
