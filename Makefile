NAME := kafka-s3-consumer

all: package

package:
	mvn package

clean:
	rm -rf *.deb
	mvn clean

deb: VERSION := $(shell mvn org.apache.maven.plugins:maven-help-plugin:2.1.1:evaluate -Dexpression=project.version | grep -Ev '(^\[|Download\w+:)')
deb: JAR_FILE := $(NAME)-$(VERSION).jar
deb: WORK_DIR := $(shell mktemp -d)
deb: TARGET_DIR := $(WORK_DIR)/usr/share/java
deb: clean package
	# Building deb for $(JAR_FILE)
	mkdir -p $(TARGET_DIR)
	cp target/$(JAR_FILE) $(TARGET_DIR)
	cd $(TARGET_DIR); ln -s $(JAR_FILE) $(NAME).jar

	fpm -t deb -s dir -n $(NAME) -C $(WORK_DIR) -v $(VERSION) \
	  --description "Kafka consumer for batching messages to upload to S3." \
	  --url "https://github.com/disqus/kafka-s3-consumer/" \
	  --deb-user root \
	  --deb-group root .

	rm -rf $(WORK_DIR)
