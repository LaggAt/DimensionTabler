-- (c) 2018 Florian Lagg <github@florian.lagg.at>
-- Under Terms of GPL v3

CREATE DATABASE `dimensiontabler_demo` /*!40100 DEFAULT CHARACTER SET utf8 */;
USE `dimensiontabler_demo`;

CREATE USER `dimensiontabler_demo`@`localhost` IDENTIFIED BY 'demo4711';
GRANT ALL ON `dimensiontabler_demo`.* TO `dimensiontabler_demo`@`localhost`;
FLUSH privileges;

-- DROP TABLE `ticker`;

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
  WHILE i <= 24*60*4 DO -- about every minute for 4 currencies
    INSERT INTO `ticker` 
    (`dt`,`currency`,`price`) VALUES (
		FROM_UNIXTIME(
			UNIX_TIMESTAMP(date_sub(now(), interval daySub day))
			+ FLOOR(RAND()*24*60*60)),
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

-- this will be the output table
CREATE TABLE `dt_ticker` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `time_sec` int(11) NOT NULL,
  `time_sec_insert` INT(11) NOT NULL,
  `time_sec_update` INT(11) NOT NULL DEFAULT 0,
  `grp_hash` VARCHAR(64) NOT NULL,
  `group_currency` varchar(45) NOT NULL,
  `wallet_id_first` int(11) NOT NULL,
  `wallet_id_last` int(11) NOT NULL,
  `price_open` decimal(18,8) DEFAULT NULL,
  `price_high` decimal(18,8) DEFAULT NULL,
  `price_low` decimal(18,8) DEFAULT NULL,
  `price_close` decimal(18,8) DEFAULT NULL,
  `price_average` DECIMAL(18,8) DEFAULT NULL,
  `var_iter` int(11) NULL,
  PRIMARY KEY (`id`),
  KEY `group_idx` (`time_sec`,`group_currency`),
  KEY `first_wallet_id_idx` (`wallet_id_first`),
  KEY `last_wallet_id_idx` (`wallet_id_last`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

