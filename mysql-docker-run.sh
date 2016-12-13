#!/bin/bash

docker rm -f adbcj-test-mysql
docker run --name adbcj-test-mysql -p 3306:3306 --env MYSQL_ROOT_PASSWORD=adbcjtck --env MYSQL_DATABASE=adbcjtck --env MYSQL_USER=adbcjtck --env MYSQL_PASSWORD=adbcjtck -d mysql