pipeline {
    agent any
        stages {
            stage('build') {
                environment {
                    LB_IP=10.156.14.138
                    LB_PORT=5000
                    ORIGIN_IP=10.156.14.138
                    ORIGIN_ID=TestOrigin
                    DIST_IP=10.156.14.13
                    DIST_ID=TestDist
                    ROOT_uname=username
                    ROOT_passwd=password
                }
                steps {
                    sh 'echo Building ${BRANCH_NAME}...'
                        sh 'python3 ./scripts/start_lb.py'
                }
            }
        }
}
