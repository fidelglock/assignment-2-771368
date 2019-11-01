1. RabbitMQ on Google Compute Engine
https://console.cloud.google.com/marketplace/details/bitnami-launchpad/rabbitmq?project=forward-subject-254912

2. 

sudo apt-get install python3-pip
pip3 install virtualenv
virtualenv bdp-ass2
source bdp-ass2/bin/activate
pip install -r requirements.txt


3. setup the FTP server:

sudo /usr/sbin/useradd ftpserver

password : ftpserver

4. update file "etc/init.d/sshd.config"

"passwordAuthentication no"
 to
"passwordAuthentication yes"

5. Run

sudo systemctl restart sshd


