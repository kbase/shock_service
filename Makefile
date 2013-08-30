TARGET ?= /kb/deployment
DEPLOY_RUNTIME = /kb/runtime
SERVICE = shock_service
SERVICE_DIR = $(TARGET)/services/$(SERVICE)
TPAGE_ARGS = --define kb_top=$(TARGET) --define service=$(SERVICE)

TPAGE_PROD_ARGS = --define site_url=http://kbase.us/services/shock \
--define api_url=http://kbase.us/services/shock	\
--define site_port=7043 \
--define api_port=7044 \
--define site_dir=/disk0/site \
--define data_dir=/disk0/data \
--define logs_dir=$(SERVICE_DIR)/logs/shock \
--define mongo_host=mongodb.kbase.us \
--define mongo_db=ShockDB

TPAGE_TEST_ARGS = --define site_port=7077 \
--define api_port=7078 \
--define site_dir=/mnt/Shock/site \
--define data_dir=/mnt/Shock/data \
--define logs_dir=/mnt/Shock/logs \
--define mongo_host=localhost \
--define mongo_db=ShockDBtest

include $(KB_TOP)/tools/Makefile.common

.PHONY : test

all: deploy

deploy: deploy-libs deploy-client deploy-service

deploy-libs:
	sh install-libs.sh $(TARGET)
	
deploy-client: deploy-service

deploy-service: initialize
	$(TPAGE) $(TPAGE_PROD_ARGS) shock.cfg.tt > shock.cfg
	sh install-service.sh $(SERVICE_DIR) $(TARGET) prod

deploy-service-test: initialize
	$(TPAGE) $(TPAGE_TEST_ARGS) shock.cfg.tt > shock.cfg
	sh install-service.sh $(SERVICE_DIR) $(TARGET) test

initialize:
	git submodule init
	git submodule update
	$(TPAGE) $(TPAGE_ARGS) init/shock.conf.tt > /etc/init/shock.conf

# Test Section
TESTS = $(wildcard test/*.t)

test:
	# run each test
	for t in $(TESTS) ; do \
		if [ -f $$t ] ; then \
			$(DEPLOY_RUNTIME)/bin/perl $$t ; \
			if [ $$? -ne 0 ] ; then \
				exit 1 ; \
			fi \
		fi \
	done

