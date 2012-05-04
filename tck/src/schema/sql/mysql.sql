-- This script must be run by a MySQL user with admin privileges

GRANT SELECT, INSERT, DELETE, UPDATE ON adbcjtck.* TO adbcjtck@localhost IDENTIFIED BY 'adbcjtck';

DROP DATABASE IF EXISTS adbcjtck;
CREATE DATABASE adbcjtck;

USE adbcjtck;

DROP TABLE IF EXISTS simple_values;
CREATE TABLE simple_values (
  int_val int,
  str_val varchar(255)
);

INSERT INTO simple_values (int_val, str_val) values (null, null);
INSERT INTO simple_values (int_val, str_val) values (0, 'Zero');
INSERT INTO simple_values (int_val, str_val) values (1, 'One');
INSERT INTO simple_values (int_val, str_val) values (2, 'Two');
INSERT INTO simple_values (int_val, str_val) values (3, 'Three');
INSERT INTO simple_values (int_val, str_val) values (4, 'Four');

DROP TABLE IF EXISTS updates;
CREATE TABLE updates (id int) type=InnoDB;

DROP TABLE IF EXISTS locks;
CREATE TABLE locks (name varchar(255) primary key not null) type=InnoDB;
INSERT INTO locks(name) VALUES ('lock');

CREATE TABLE IF NOT EXISTS table_with_some_values (
  auto_int int(11) NOT NULL AUTO_INCREMENT,
  can_be_null_int int(11) DEFAULT NULL,
  can_be_null_varchar varchar(255) DEFAULT NULL,
  PRIMARY KEY (auto_int)
) ENGINE=InnoDB  DEFAULT CHARSET=latin1 AUTO_INCREMENT=3 ;

--
-- Dumping data for table 'table_with_some_values'
--

INSERT INTO table_with_some_values (auto_int, can_be_null_int, can_be_null_varchar) VALUES
(1, NULL, NULL),
(2, 42, '42');

CREATE TABLE IF NOT EXISTS `supporteddatatypes` (
  `intValue` int(11) NOT NULL,
  `varChar` varchar(255) NOT NULL,
  `bigInt` bigint(20) NOT NULL,
  `decimal` decimal(10,0) NOT NULL,
  `date` date NOT NULL,
  `double` double NOT NULL
) ENGINE=MyISAM DEFAULT CHARSET=latin1;


INSERT INTO `adbcjtck`.`supportedDataTypes` (
  `intValue` ,
  `varChar` ,
  `bigInt` ,
  `decimal` ,
  `date` ,
  `double`
)
VALUES (
'42', '4242', '42', '42', '2012-05-03', '42.42'
);