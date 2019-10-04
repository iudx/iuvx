pipeline {
    agent any
        stages {
            stage('build') {
                environment {
                    LB_IP = "10.156.14.138"
                    HTTP_PORT = "5000"
                    ORIGIN_IP = "10.156.14.138"
                    DIST_IP = "10.156.14.138"
                }
                steps {
                    sh 'echo Building ${BRANCH_NAME}...'
                        sh 'python3 ./scripts/start_lb.py'
                }
            }
        }
}
