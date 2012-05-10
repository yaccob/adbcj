
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

DROP TABLE IF EXISTS table_with_some_values;
CREATE TABLE IF NOT EXISTS table_with_some_values (
  auto_int int(11) NOT NULL AUTO_INCREMENT,
  can_be_null_int int(11) DEFAULT NULL,
  can_be_null_varchar varchar(255) DEFAULT NULL,
  PRIMARY KEY (auto_int)
) ENGINE=InnoDB  DEFAULT CHARSET=UTF8 AUTO_INCREMENT=3 ;


INSERT INTO table_with_some_values (auto_int, can_be_null_int, can_be_null_varchar) VALUES
(1, NULL, NULL),
(2, 42, '42');

DROP TABLE IF EXISTS supporteddatatypes;
CREATE TABLE IF NOT EXISTS `supporteddatatypes` (
  `intValue` int(11) NOT NULL,
  `varChar` varchar(255) NOT NULL,
  `bigInt` bigint(20) NOT NULL,
  `decimal` decimal(10,0) NOT NULL,
  `date` date NOT NULL,
  `double` double NOT NULL
) ENGINE=MyISAM DEFAULT CHARSET=UTF8;


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


DROP TABLE IF EXISTS textcontent;

CREATE TABLE IF NOT EXISTS textcontent (
  id int(11) NOT NULL AUTO_INCREMENT,
  lang varchar(16) COLLATE utf8_roman_ci NOT NULL,
  textData varchar(255) COLLATE utf8_roman_ci NOT NULL,
  PRIMARY KEY (id)
) ENGINE=InnoDB  DEFAULT CHARSET=utf8 COLLATE=utf8_roman_ci AUTO_INCREMENT=6 ;


INSERT INTO textContent (id, lang, textData) VALUES
(1, 'kr', '한국어 너무 좋다'),
(2, 'en', 'English is a nice language'),
(3, 'de', 'Die äüö sind toll'),
(4, 'zh', '维基百科（英语：Wikipedia）'),
(5, 'ja', 'ウィキペディア（英: Wikipedia）');
