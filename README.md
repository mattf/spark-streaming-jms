echo "auth=no" >> /etc/qpid/qpidd.conf

systemctl start qpidd

qpid-config add queue default

--

sbt "run-main com.redhat.summitdemo.scanner.ScannerSimulator"

sbt "run-main com.redhat.summitdemo.scanner.ScannerDataProcessor"
