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
  * tiny integers are no more automaticly decoded as booleans

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
