set -e
mvn install
mvn scalastyle:check
mvn test        
