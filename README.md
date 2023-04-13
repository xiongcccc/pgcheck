## Introduce

pgcheck is a one-click tool to get the running status of PostgreSQL, including stream replication/lock/wait events/partition/index/relation,etc., which makes the operation and maintenance more efficient.

## Usage

~~~shell
[postgres@xiongcc pgcheck_tool]$ ./pgcheck 
Description: The script is used to collect specified information
Usage:
 ./pgcheck relation database schema         : list information about tables and indexes in the specified schema
 ./pgcheck alltoast database schema         : list all toasts and their corresponding tables
 ./pgcheck reltoast database relname        : list the toast information of the specified table
 ./pgcheck dbstatus database                : list all database status and statistics
 ./pgcheck index_bloat database             : index bloat information (estimated value)
 ./pgcheck index_duplicate database         : index duplicate information
 ./pgcheck index_low database               : index low efficiency information
 ./pgcheck index_state database             : index detail information
 ./pgcheck lock database                    : lock wait queue and lock wait state
 ./pgcheck checkpoint database              : background and checkpointer state
 ./pgcheck freeze database                  : database transaction id consuming state
 ./pgcheck replication database             : streaming replication (physical) state
 ./pgcheck connections database             : database connections and current query
 ./pgcheck long_transaction database        : long transaction detail
 ./pgcheck relation_bloat database          : relation bloat information (estimated value)
 ./pgcheck vacuum_state database            : current vacuum progress information
 ./pgcheck index_create database            : index create progress information
 ./pgcheck wal_archive database             : wal archive progress information
 ./pgcheck wal_generate database wal_path   : wal generate speed (you should provide extra wal directory)
 ./pgcheck wait_event database              : wait event and wait event type
 ./pgcheck partition database               : native and inherit partition info (estimated value)
 ./pgcheck object database user             : get the objects owned by the user in the specified database
 ./pgcheck --help or -h                     : print this help information

 Author: xiongcc@PostgreSQL学徒, github: https://github.com/xiongcccc.
 If you have any feedback or suggestions, feel free to contace with me.
 Email: xiongcc_1994@126.com/xiongcc_1994@outlool.com. Wechat: _xiongcc
~~~



