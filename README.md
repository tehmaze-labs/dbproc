DBProc
======

Mappings for easy access to (database) stored procedures in Python.


Database support
================

Currently the following database types are supported:

  * [MySQL](http://www.mysql.com/), [MariaDB](http://mariadb.org/),
    [Percona](http://www.percona.com/), using the
    [MySQL-python](http://mysql-python.sourceforge.net/) DB API 2.0 module
  * [PgSQL](http://www.postgres.org/), using the
    [psycopg2](http://initd.org/psycopg/) DB API 2.0 module


Example
=======

Firstly, we can declare a PL/pgSQL stored procedure as follows:

```SQL
   CREATE OR REPLACE FUNCTION test_dbproc_add(a INTEGER, b INTEGER)
       RETURNS INTEGER AS $$
   BEGIN
       RETURN $1 + $2;
   END;
   $$ LANGUAGE 'plpgsql';
```

Now, using [wrap](http://dbproc.readthedocs.org/#module-dbproc), we can
access this stored procedure in Python like:

```Python
   >>> import psycopg2
   >>> from dbproc import Wrap
   >>> conn = psycopg2.connect('dbname=test')
   >>> proc = Wrap(conn, prefix='test_dbproc_')
   >>> print proc.add(23, 42)
   65
```

Bugs/Features
=============

You can issue a ticket in GitHub: https://github.com/tehmaze/dbproc
