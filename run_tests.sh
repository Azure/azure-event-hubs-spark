set -e
mvn scalastyle:test
mvn package
mvn test        
