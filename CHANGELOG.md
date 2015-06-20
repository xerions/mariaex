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
