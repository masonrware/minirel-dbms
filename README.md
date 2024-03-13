# minirel-dbms
This is the codebase for a working single-user DBMS that can execute certain simple SQL queries. The DBMS is implemented in layers: with the topmost layer being a parser of SQL queries, and the lowermost layer being a disk I/O layer which reads and writes pages from and to the disk (UNIX fs).
