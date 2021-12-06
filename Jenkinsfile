#!/usr/bin/env groovy

def config = jobConfig {
	nodeLabel = 'docker-debian-jdk11'
    slackChannel = 'csid-build'
}

def publishStep(String vaultSecret) {
    withVaultFile([["gradle/${vaultSecret}", "settings_file", "${env.WORKSPACE}/init.gradle", "GRADLE_NEXUS_SETTINGS"]]) {
        sh "./gradlew --init-script ${GRADLE_NEXUS_SETTINGS} --no-daemon publish"
    }
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

    if (config.publish) {
		stage("Publish to artifactory") {
			if (config.isDevJob) {
				publishStep('artifactory_snapshots_settings')
			}
		}
	}
}

runJob config, job
