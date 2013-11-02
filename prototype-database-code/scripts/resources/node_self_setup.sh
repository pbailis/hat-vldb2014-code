chmod 600 .ssh/id_rsa
sudo apt-get update
sudo apt-get -q -y install git
sudo apt-get -q -y install maven
sudo apt-get -q -y install openjdk-6-jdk
export JAVA_HOME=/usr/lib/jvm/java-6-openjdk-amd64
git clone git@github.com:pbailis/thebes.git
cd thebes/thebes-code/
# git checkout ec2-experiment
git pull
mvn package