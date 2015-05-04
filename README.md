    echo "auth=no" >> /etc/qpid/qpidd.conf
    systemctl start qpidd
    qpid-config add queue default

--

    sbt "run-main com.redhat.summitdemo.scanner.ScannerSimulator"
    sbt "run-main com.redhat.summitdemo.scanner.ScannerDataProcessor"

--

    sbt assembly
    spark-submit --jars target/scala-2.10/spark-streaming-jms-assembly-0.0.1.jar example.py
