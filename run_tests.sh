set -e
mvn scalastyle:check
mvn package
mvn test        
