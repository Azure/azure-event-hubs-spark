set -e
mvn scalastyle:test
mvn clean install -DskipTests=true                                                                        
mvn test        
