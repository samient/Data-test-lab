pipeline {
    agent any
   
    stages {
        stage('Checkout') {
            steps {
                echo "Current Stage: ${env.STAGE_NAME}"
                // Your checkout logic here
                checkout scm
            }
        }
       
        stage('Build') {
            steps {
                echo "Current Stage: ${env.STAGE_NAME}"
                // Your build logic here
                sh 'echo "Building the application..."'
            }
        }
       
        stage('Test') {
            steps {
                echo "Current Stage: ${env.STAGE_NAME}"
                // Your test logic here
                sh 'echo "Running tests..."'
            }
        }
       
        stage('Security Scan') {
            steps {
                echo "Current Stage: ${env.STAGE_NAME}"
                // Your security scan logic here
                sh 'echo "Running security scans..."'
            }
        }
       
        stage('Package') {
            steps {
                echo "Current Stage: ${env.STAGE_NAME}"
                // Your packaging logic here
                sh 'echo "Packaging application..."'
            }
        }
       
        stage('Deploy to Staging') {
            steps {
                echo "Current Stage: ${env.STAGE_NAME}"
                // Your staging deployment logic here
                sh 'echo "Deploying to staging environment..."'
            }
        }
       
        stage('Integration Tests') {
            steps {
                echo "Current Stage: ${env.STAGE_NAME}"
                // Your integration test logic here
                sh 'echo "Running integration tests..."'
            }
        }
       
        stage('Deploy to Production') {
            when {
                branch 'main'
            }
            steps {
                echo "Current Stage: ${env.STAGE_NAME}"
                // Your production deployment logic here
                sh 'echo "Deploying to production environment..."'
            }
        }
    }
   
    post {
        always {
            echo "Pipeline completed"
            echo "All stages executed in this pipeline run"
        }
        success {
            echo "Pipeline succeeded!"
        }
        failure {
            echo "Pipeline failed!"
        }
    }
}
