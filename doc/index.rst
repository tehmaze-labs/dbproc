.. DBProc documentation master file, created by
   sphinx-quickstart2 on Sat Feb 22 01:49:59 2014.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to DBProc's documentation!
==================================

Example
=======

Firstly, we can declare a PL/pgSQL stored procedure as follows::

   CREATE OR REPLACE FUNCTION test_dbproc_add(a INTEGER, b INTEGER)
       RETURNS INTEGER AS $$
   BEGIN
       RETURN $1 + $2;
   END;
   $$ LANGUAGE 'plpgsql';

Now, using :class:`dbproc.Wrap`, we can access this stored procedure in Python
like::

   >>> import psycopg2
   >>> from dbproc import DBProc
   >>> conn = psycopg2.connect('dbname=test')
   >>> proc = DBProc(conn, prefix='test_dbproc_')
   >>> print proc.add(23, 42)
   65

Bugs/Features
=============

You can issue a ticket in GitHub: https://github.com/tehmaze/dbproc

API
===

.. automodule:: dbproc
   :members:

.. automodule:: dbproc.backend
   :members:

.. automodule:: dbproc.backend.base
   :members:

.. automodule:: dbproc.backend.mysql
   :members:

.. automodule:: dbproc.backend.pgsql
   :members:

.. automodule:: dbproc.wrap
   :members:


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

