# oasys-sdk-python <span><img src="http://slurmed.com/fanart/javier/213_purple-fruit-snake.gif" alt="Purple Fruit Snake" width="100"/></span>
SDK for python based apps running within oasys.


## Quickstart

To create a python app for the oasys platform:
 
 1. [fork this repository](https://guides.github.com/activities/forking/), or create a new repository and copy this repo's source code into it.
  
 2. Update the `name` field in `setup.py` to your application name (as registered on oasys), and apply a version number. Any version numbers are valid within oasys, but we strongly recommend either [semantic versioning](http://semver.org) convention, or using the git hash of the currently checked out version (see `git rev-parse HEAD`)
 
 3. Connect your repo to the oasys platform using our [github integration](). If you can't do that (e.g. if your repository is behind a firewall onsite), no problem - create a slug of the application code with: 
 ```bash
    git clone --depth 1 git@server:repo.git $DEST
    rm -r $DEST/.git
    tar czvf repo.tgz $DEST
    rm -rf $DEST
 ```
 Then upload the slug to the application creation wizard on the oasys platform.
 
 4. ...TODO

