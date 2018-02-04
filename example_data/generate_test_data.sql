CREATE DATABASE `dimensiontabler_demo` /*!40100 DEFAULT CHARACTER SET utf8 */;

CREATE USER `dimensiontabler_demo`@`localhost` IDENTIFIED BY 'demo4711';
GRANT SELECT ON `dimensiontabler_demo`.* TO `dimensiontabler_demo`@`localhost`;
FLUSH privileges;

DROP TABLE `ticker`;

CREATE TABLE `ticker` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT 'Unique identifier',
  `currency` varchar(5) NOT NULL COMMENT 'name of currency for this ticker value',
  `dt` datetime NOT NULL COMMENT 'date and time',
  `price` decimal(18,8) DEFAULT NULL COMMENT 'current price',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

DELIMITER $$
CREATE PROCEDURE generate_demo_data(in daySub INT)
BEGIN
  DECLARE i INT DEFAULT 0;
  WHILE i < 24*60*4 DO -- about every minute for 4 currencies
    INSERT INTO `ticker` 
    (`dt`,`currency`,`price`) VALUES (
		FROM_UNIXTIME(
			UNIX_TIMESTAMP(date_sub(now(), interval daySub day))
			+ FLOOR(RAND()*7*24*60*60)),
        ELT(
			FLOOR(RAND()*4)+1, 
			'BTC', 'ETH', 'LTC', 'IOTA'),
		ROUND(RAND()*10,8)
    );
    SET i = i + 1;
  END WHILE;
END$$
DELIMITER ;

-- 40320 lines of test data for last 7 days
CALL generate_demo_data(7);
CALL generate_demo_data(6);
CALL generate_demo_data(5);
CALL generate_demo_data(4);
CALL generate_demo_data(3);
CALL generate_demo_data(2);
CALL generate_demo_data(1);

DROP PROCEDURE generate_demo_data;
