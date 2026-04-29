# Spring Boot 3.5 requires Java 17+. This workspace has JDK 21 installed here.
$env:JAVA_HOME="D:\java\jdk-21"
$env:PATH="$env:JAVA_HOME\bin;$env:PATH"

$env:MYSQL_HOST="192.168.2.162"
$env:MYSQL_PORT="3306"
$env:MYSQL_DATABASE="zjsm"
$env:MYSQL_USERNAME="root"
$env:MYSQL_PASSWORD="123456"
$env:REDIS_HOST="192.168.2.162"
$env:REDIS_PORT="6379"

mvn spring-boot:run
