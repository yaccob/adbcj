# Asynchronous Database Connectivity in Java (ADBCJ)
ADBCJ allows you to access a relational database in a asynchronous, non-blocking fashion. 
The API is inspired by JDBC, but makes all calls asynchronous. 

The asynchronous access prevents any blocked threads, 
which just wait for the result of the database. 

It also allows to pipeline operations, which are independent.
 Depending on the application, this can give a significant performance gain.

ADBCJ is intended as low level foundation. 
Therefore it's is written in Java, so other languages like Scala, Groovy, Kotlin etc can cosume it to

## Documentation:
I'm working on it: https://github.com/gamlerhart/adbcj/wiki
