#!/bin/bash

os="$OSTYPE"

echo "$os"

install_pipenv_for_ubuntu() {
    sudo apt-get install -y python-pip
    pip install --user pipenv
}

install_pipenv_for_mac() {
    brew install pipenv
}
​
# install pipenv
# for ubuntu
if [[ "$os" == "linux-gnu" ]]; then
    install_pipenv_for_ubuntu
# for mac OS
elif [[ "$os" == "darwin"* ]]; then
    install_pipenv_for_mac
fi

# move to project folder
cd $PWD
​
echo "using pipenv"
# configure to use python 3.7
pipenv --python 3.7
​
# install dependencies
pipenv install aiokafka