# Spring Boot 3.5 requires Java 17+.
$env:JAVA_HOME="D:\java\jdk-21"
$env:PATH="$env:JAVA_HOME\bin;$env:PATH"

$env:MYSQL_HOST="localhost"
$env:MYSQL_PORT="3306"
$env:MYSQL_DATABASE="big_data_case_study"
$env:MYSQL_USERNAME="bigdata"
$env:MYSQL_PASSWORD="BigData@123456"
$env:REDIS_HOST="localhost"
$env:REDIS_PORT="6379"

Write-Host "Starting backend: http://localhost:8001/user_profile"
Write-Host "Start the Vue frontend separately from frontend-vue with: npm run dev"

mvn spring-boot:run
