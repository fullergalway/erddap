rem This is the Windows batch file to run DasDds.
rem See http://coastwatch.pfeg.noaa.gov/erddap/download/setupDatasetsXml.html#Tools

java -cp ./classes;../../../lib/servlet-api.jar;lib/activation.jar;lib/axis.jar;lib/cassandra-driver-core.jar;lib/netty-all.jar;lib/guava.jar;lib/metrics-core.jar;lib/lz4.jar;lib/snappy-java.jar;lib/commons-compress.jar;lib/commons-discovery.jar;lib/itext-1.3.1.jar;lib/jaxrpc.jar;lib/joda-time.jar;lib/lucene-core.jar;lib/mail.jar;lib/netcdfAll-latest.jar;lib/slf4j.jar;lib/postgresql.jdbc.jar;lib/saaj.jar;lib/wsdl4j.jar;lib/aws-java-sdk.jar;lib/commons-codec.jar;lib/commons-logging.jar;lib/fluent-hc.jar;lib/httpclient.jar;lib/httpclient-cache.jar;lib/httpcore.jar;lib/httpmime.jar;lib/jna.jar;lib/jna-platform.jar;lib/jackson-annotations.jar;lib/jackson-core.jar;lib/jackson-databind.jar  -Xms1000M -Xmx1000M  gov.noaa.pfel.erddap.DasDds %1 %2 %3 %4 %5 %6 %7 %8 %9