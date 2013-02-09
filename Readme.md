# Asynchronous Database Connectivity in Java (ADBCJ)
ADBCJ allows you to access a relational database in a asynchronous, non-blocking fashion. The API is inspired by JDBC, but makes all calls asynchronous. 

The asynchronous access prevents any blocked threads, which just wait for the result of the database. It also allows to pipeline operations, which are independent. Depending on the application, this can give a significat performance gain.

## Documentation:
Upcoming, corrently check these minimal blog-posts out: http://www.gamlor.info/wordpress/tag/adbcj/
