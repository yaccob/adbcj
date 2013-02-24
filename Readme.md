# Asynchronous Database Connectivity in Java (ADBCJ)
ADBCJ allows you to access a relational database in a asynchronous, non-blocking fashion. The API is inspired by JDBC, but makes all calls asynchronous. 

The asynchronous access prevents any blocked threads, which just wait for the result of the database. It also allows to pipeline operations, which are independent. Depending on the application, this can give a significat performance gain.

ADBCJ is intended as low level foundation. The language limitations of Java make it hard to dealy with fine grained, asynchronous operations. We recommend to use language and decorator library which makes the API fit for the specific task. Like for Scala: 

## Documentation:
I'm working on it: https://github.com/gamlerhart/adbcj/wiki
