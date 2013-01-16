CREATE TABLE `rabins` (
  `srcid` varchar(20)  DEFAULT '0',
  `ip_s` int(10) unsigned NOT NULL DEFAULT '0' COMMENT 'IP Source Address',
  `ip_d` int(10) unsigned NOT NULL DEFAULT '0' COMMENT 'IP Destination Address',
  `protocol` tinyint(3) unsigned NOT NULL DEFAULT '0' COMMENT 'IP Protocol - see table protocols',
  `port_s` smallint(5) unsigned NOT NULL DEFAULT '0' COMMENT 'Port Source',
  `port_d` smallint(5) unsigned NOT NULL DEFAULT '0' COMMENT 'Port Destination',
  `starttime` float(9,6) NOT NULL COMMENT 'DateTime this flow began',
  `endtime` float(9,6) NOT NULL COMMENT 'DateTime this flow ended',
  `dur` float(9,6) NOT NULL,
  `bytes` bigint(20) unsigned NOT NULL DEFAULT '0' COMMENT 'Bytes in this flow',
  `packets` int(10) unsigned NOT NULL DEFAULT '0' COMMENT 'Packets in this flow',
  `retrans` int(10) unsigned NOT NULL DEFAULT '0',
  `loadt` int(10) unsigned NOT NULL DEFAULT '0', 
  `dom_s` int(10) unsigned NOT NULL DEFAULT '0' COMMENT 'Domain Source',
  `dom_d` int(10) unsigned NOT NULL DEFAULT '0' COMMENT 'Domain_Dest',
  `as_s` int(10) unsigned NOT NULL DEFAULT '0' COMMENT 'AsNum Source',
  `as_d` int(10) unsigned NOT NULL DEFAULT '0' COMMENT 'AsNum Dest'
) ENGINE=MyISAM  DEFAULT CHARSET=utf8;
