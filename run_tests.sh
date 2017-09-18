set -e
mvn install -DskipTests
mvn scalastyle:check
mvn -q test        
