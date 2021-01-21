
@Library('dst-shared@release/shasta-1.4') _

dockerBuildPipeline {
    repository = "cray"
    imagePrefix = "cray"
    app = "boa"
    name = "boa"
    description = "Cray management system boot orchestration agent"
    product = "csm"
    enableSonar = false
    sendEvents = ["BOA"]
    
    githubPushRepo = "Cray-HPE/boa"
    /*
        By default all branches are pushed to GitHub

        Optionally, to limit which branches are pushed, add a githubPushBranches regex variable
        Examples:
        githubPushBranches =  /master/ # Only push the master branch
        
        In this case, we push bugfix, feature, hot fix, master, and release branches
    */
    githubPushBranches =  /(bugfix\/.*|feature\/.*|hotfix\/.*|master|release\/.*)/ 
}
