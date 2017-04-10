node('docker') {
    timestamps {
        checkout([$class: 'GitSCM',
                  branches: scm.branches,
                  extensions: [[$class: 'CloneOption', depth: 0, noTags: true, reference: '', shallow: true]],
                  userRemoteConfigs: scm.userRemoteConfigs
        ])

        def image
        stage('Packaging') {
            image = docker.build "se-artif-prd.infinera.com/webapps/registrator-swarm:v7.2"
        }
        stage('Publishing') {
            image.push()
        }
    }
}
