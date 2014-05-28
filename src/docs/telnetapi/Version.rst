=======
Version
=======

The version command returns the product name (KairosDB) and its version.

::

 version /n

The output looks like this

::

  KairosDB
  0.9.4


Here is an simple example using netcat.

::

  echo "version" | nc -w 30 localhost 4242
