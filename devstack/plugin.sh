
#!/bin/bash

if [[ "$1" == "stack" && "$2" == "install" ]]; then
    cd
    rm -rf red-cinder-driver
    git clone git@github.com:qeas/red-cinder-driver.git
    mkdir -p /opt/stack/cinder/cinder/volume/drivers/red
    cp -rf red-cinder-driver/* /opt/stack/cinder/cinder/volume/drivers/red/
fi

