# 0.8.2

* Enhancements
  * remove warnings for elixir v1.4

* Bug fixes
  * fix some bugs on reconnection

# 0.8.1

* Enhancements
  * better performance on decoding multiple rows
  * add support for non-microsecond datetimes

* Bug fixes
  * fix error when executing a stored procedure that does not return any results

# 0.8.0

* Enhancements
  * add MDBPORT enviroment
  * add streaming support

* Backwards incompatible changes
  * remove support of elixir < 1.2

# 0.7.9

* Enhancements
  * different performance optimization of query handling

* Bug fixes
  * fix truncate query

# 0.7.8

* Enhancements
  * add tls support
  * add option for switching, how strings are saved in database

# 0.7.7

* Enhancements
  * do not checkout or activate on ping if buffer is full

# 0.7.6

* Enhancements
  * normalize port, if it is given as string
  * update to db_connection v1.0.0-rc

* Bug fixes
  * add missing crypto to dependencies

# 0.7.5

* Enhancements
  * support writing of latin1 tables

# 0.7.4

* Bug fixes
  * make function for getting formated version more robust to different formats

# 0.7.3

* Bug fixes
  * clean state on end of execution

# 0.7.2

* Bug fixes
  * fix backwards compatibility for ecto 1.1
  * fix wrong handling of savepoint, extra prepare, execute command

# 0.7.1

* Bug fixes
  * fix tcp connect lost handshake packet

# 0.7.0

* Enhancements
  * reimplement protocol based on db_connection library
  * add possibility to decode bits to support old type booleans

* Bug fixes
  * fix memory overconsuming

# 0.6.4

* Enhancements
  * do not hold references to last executed query in a connection process

# 0.6.3

* Bug fixes
  * remove compilation output, which accidental was there

# 0.6.2

* Bug fixes
  * fix version handling for complexer MySQL versions
  * add connection to list of dependent applications
  * support parameterized replace commands

# 0.6.1

* Enhancements
  * add support for MySQL 5.7.X

# 0.6.0

* Enhancements
  * add decoding of values on client side
  * add sync connect
  * use connection library instead of GenServer

* Bug fixes
  * fix support command

* Backwards incompatible changes
  * remove async_query

# 0.5.0

* Enhancements
  * add async_query

* Backwards incompatible changes
  * depends on elixir 1.1, because of GenServer.whereis/1

# 0.4.3

* Enhancements
  * add possibility to start databaseless connection

# 0.4.2

* Enhancements
  * Revert client side decoding as affects performance negatively

# 0.4.1

* Enhancments
  * decode values on client side
  * add possibility to do decoding later

# 0.4.0

* Bug fixes
  * add SHOW to supported prepared statements commands

* Backwards incompatible changes
  * rows returned as lists, not as tuples anymore

# 0.3.2

* Bug fixes
  * fix result statement for ping, if no ping needed

# 0.3.1

* Enhancments
  * add keepalive to connection, per default is disabled

* Bug fixes
  * fix parsing of time 00-00-00

# 0.3.0

* Bug fixes
  * fix parsing of date 0000-00-00
  * allow saving dates, that have year less as 1000, due time is another format as date

* Backwards incompatible changes
  * tiny integers are no more automatically decoded as booleans

# 0.2.2

* Bug fixes
  * do not close statement, if it is cached, and latest used too

# 0.2.1

* Bug fixes
  * clean statement_id before running, which have produced close statement twice

# 0.2.0

* Enhancments
  * add caching and reusing of prepared statements

* Backwards incompatible changes
  * no more sanitizing input, the queris starting with whitespace and '\n' are no more valid queries

# 0.1.7

* Enhancments
  * initial implementation of stored procedures
  * add year, smallint and mediumint decoding
  * allow query command delimiter be \n
  * allow setting :sndbuf, :recbuf, :buffer in tcp options

# 0.1.6

* Bug fixes
  * Queue is not dequeued

# 0.1.5

* Enhancments
  * Allow add own :socket_options

* Bug fixes
  * strip statement

# 0.1.4

* Bug fixes
  * fix regression, that doesn't handle the plugin type right on handshake

# 0.1.3

* Bug fixes
  * close prepared statements on end of execution

# 0.1.2

* Bug fixes
  * fix crashing of a process on stop
  * fix authorization with mysql 5.1
  * fix wrong calculated padding in null bitmap

# 0.1.1

* Enhancements
  * Add possibility to define charset, and add default charset utf8

# 0.1.0

First release!
