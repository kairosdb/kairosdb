=======
Version
=======

The version command returns the product name (KairosDB) and its version.

::

 version \n

The output looks like this

::

  KairosDB
  1.0.0


Here is an simple example using netcat.

.. code-block:: sh

  echo "version" | nc -w 30 localhost 4242
