FROM eclipse-temurin:17-jre-alpine

ARG JMETER_VERSION=5.6.3
ENV JMETER_VERSION=${JMETER_VERSION} \
	JMETER_BASE=/opt/apache-jmeter-${JMETER_VERSION} \
	JMETER_HOME=/opt/apache-jmeter \
	HEAP="-Xms256m -Xmx512m"

RUN set -eux; \
	apk add --no-cache curl bash tar; \
	curl -fsSL "https://downloads.apache.org/jmeter/binaries/apache-jmeter-${JMETER_VERSION}.tgz" -o /tmp/jmeter.tgz || \
	  curl -fsSL "https://archive.apache.org/dist/jmeter/binaries/apache-jmeter-${JMETER_VERSION}.tgz" -o /tmp/jmeter.tgz; \
	tar -xf /tmp/jmeter.tgz -C /opt; \
	ln -s ${JMETER_BASE} ${JMETER_HOME}; \
	rm /tmp/jmeter.tgz; \
	adduser -D -h /home/jmeter jmeter; \
	chown -R jmeter:jmeter ${JMETER_BASE}; \
	mkdir -p /tests; \
	chown jmeter:jmeter /tests; \
	# Install johrstrom prometheus plugin
	VER=0.6.0; \
	curl -fsSL "https://search.maven.org/remotecontent?filepath=com/github/johrstrom/jmeter-prometheus-plugin/${VER}/jmeter-prometheus-plugin-${VER}.jar" -o "${JMETER_HOME}/lib/ext/jmeter-prometheus-plugin-${VER}.jar"; \
	echo "Installed JMeter ${JMETER_VERSION} with johrstrom plugin ${VER}";

ENV PATH="${JMETER_HOME}/bin:${PATH}" \
	JMETER_HOME=${JMETER_HOME}

USER jmeter

WORKDIR /tests

ENTRYPOINT ["/bin/sh","-c"]
CMD ["jmeter -n -t /tests/test.jmx -Jprometheus.port=9270 -Jprometheus.ip=0.0.0.0 -Jjmeter.save.saveservice.output_format=csv -l /tests/results.jtl"]
