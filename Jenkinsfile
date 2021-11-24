#!/usr/bin/env groovy

def config = jobConfig {
	nodeLabel = 'docker-debian-jdk11'
    slackChannel = 'csid-build'
}

def job = {
	withDockerServer([uri: dockerHost()]) {
	    stage('Build') {
            echo 'Building with gradle...'
			try {
				sh './gradlew clean build test --fail-fast --no-daemon'
			} finally {
				junit '**/build/test-results/**/TEST-*.xml' //make the junit test results available in any case (success & failure)
			}
		}
    }
}

runJob config, job
