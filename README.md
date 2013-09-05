shock_service
=============

Enables deployment of shock server and clients within the kbase release environment

The following are instructions on how to deploy the Shock service in KBase from launching a fresh KBase instance to starting the service...

- Create security group for Shock server (this isn't required with the test ports in the Makefile but makes it convenient if you want to change the ports later)
- Using this security group and your key, launch a KBase instance
- Get code:<br />
sudo -s<br />
cd /kb<br />
git clone kbase@git.kbase.us:dev_container.git<br />
cd dev_container/modules<br />
(NOTE: currently using https://github.com/jaredbischof/shock_service.git in next command until I get admin rights to github.com/kbase/shock_service repo)<br />
git clone https://github.com/kbase/shock_service.git<br />
cd shock_service<br />

- Edit Makefile if you're not deploying in production (which you probably aren't) so that the deploy target deploys just the libs and the service-test targets:<br />
deploy: deploy-libs deploy-service-test

- Create a mongo data directory on the mounted drive and symbolic link to point to it:<br />
cd /mnt<br />
mkdir db<br />
cd /<br />
mkdir data<br />
cd data/<br />
ln -s /mnt/db .<br />

- Start mongod (preferably in screen session):<br />
cd /kb/dev_container<br />
./bootstrap /kb/runtime<br />
source user-env.sh<br />
mongod<br />

(NOTE: mongod needs a few minutes to start.  Once you can run "mongo" from the command line and connect to mongo db, then proceed with deploying Shock)
- Deploy Shock:<br />
cd /kb/dev_container<br />
./bootstrap /kb/runtime<br />
source user-env.sh<br />
make deploy<br />
service shock restart (restart Shock)<br />

- After deployment has completed, if you've associated an IP with your instance you should be able to confirm that Shock is running by going to either url below (ports are defined in shock.cfg):<br />
site ->  http://[Shock Server IP]:7077/<br />
api  ->  http://[Shock Server IP]:7078/<br />
