API Reference
=============

:class:`~pypey.Pype`'s methods can be grouped according to whether they are lazy or eager vs whether they are deferred
or immediate:

+------------+----------------------------------+----------------------------------+
|            | Deferred                         | Immediate                        |
+============+==================================+==================================+
| **Lazy**   | :func:`~pypey.Pype.accum`        | :func:`~pypey.Pype.do`           |
|            | :func:`~pypey.Pype.broadcast`    | :func:`~pypey.Pype.map`          |
|            | :func:`~pypey.Pype.cat`          | :func:`~pypey.Pype.print`        |
|            | :func:`~pypey.Pype.chunk`        | :func:`~pypey.Pype.to_file`      |
|            | :func:`~pypey.Pype.clone`        |                                  |
|            | :func:`~pypey.Pype.cycle`        |                                  |
|            | :func:`~pypey.Pype.dist`         |                                  |
|            | :func:`~pypey.Pype.do`           |                                  |
|            | :func:`~pypey.Pype.drop`         |                                  |
|            | :func:`~pypey.Pype.drop_while`   |                                  |
|            | :func:`~pypey.Pype.enum`         |                                  |
|            | :func:`~pypey.Pype.flat`         |                                  |
|            | :func:`~pypey.Pype.flatmap`      |                                  |
|            | :func:`~pypey.Pype.it`           |                                  |
|            | :func:`~pypey.Pype.interleave`   |                                  |
|            | :func:`~pypey.Pype.map`          |                                  |
|            | :func:`~pypey.Pype.partition`    |                                  |
|            | :func:`~pypey.Pype.pick`         |                                  |
|            | :func:`~pypey.Pype.print`        |                                  |
|            | :func:`~pypey.Pype.reject`       |                                  |
|            | :func:`~pypey.Pype.select`       |                                  |
|            | :func:`~pypey.Pype.slice`        |                                  |
|            | :func:`~pypey.Pype.split`        |                                  |
|            | :func:`~pypey.Pype.take`         |                                  |
|            | :func:`~pypey.Pype.take_while`   |                                  |
|            | :func:`~pypey.Pype.tee`          |                                  |
|            | :func:`~pypey.Pype.to_file`      |                                  |
|            | :func:`~pypey.Pype.uniq`         |                                  |
|            | :func:`~pypey.Pype.window`       |                                  |
|            | :func:`~pypey.Pype.zip`          |                                  |
|            | :func:`~pypey.Pype.zip_with`     |                                  |
+------------+----------------------------------+----------------------------------+
| **Eager**  | :func:`~pypey.Pype.divide`       | :func:`~pypey.Pype.eager`        |
|            | :func:`~pypey.Pype.freqs`        | :func:`~pypey.Pype.reduce`       |
|            | :func:`~pypey.Pype.group_by`     | :func:`~pypey.Pype.size`         |
|            | :func:`~pypey.Pype.reverse`      |                                  |
|            | :func:`~pypey.Pype.roundrobin`   |                                  |
|            | :func:`~pypey.Pype.sample`       |                                  |
|            | :func:`~pypey.Pype.shuffle`      |                                  |
|            | :func:`~pypey.Pype.sorted`       |                                  |
|            | :func:`~pypey.Pype.take`         |                                  |
|            | :func:`~pypey.Pype.to_json`      |                                  |
|            | :func:`~pypey.Pype.top`          |                                  |
|            | :func:`~pypey.Pype.unzip`        |                                  |
+------------+----------------------------------+----------------------------------+

Lazy methods would normally be deferred and eager methods immediate. However, if an eager method returns a pipe, Pypey
defers its execution until it's iterated through, at which point, the backing ``Iterable`` will be consumed, if lazy.

:func:`~pypey.Pype.do`, :func:`~pypey.Pype.map`, :func:`~pypey.Pype.print` and  :func:`~pypey.Pype.to_file` are
intrinsically lazy but they can be eager in certain circumstances. The three side-effectful methods
:func:`~pypey.Pype.do`, :func:`~pypey.Pype.print` and :func:`~pypey.Pype.to_file` can be made eager by setting their
``now`` parameter to ``True`` (:func:`~pypey.Pype.to_file`'s and :func:`~pypey.Pype.print`'s are ``True`` by default)
as they are often the last operations in a pipeline, when consumption of its backing ``Iterable`` is warranted.
:func:`~pypey.Pype.do` and :func:`~pypey.Pype.map` consume their iterables when they are made parallel, if their
``workers`` parameter is set to a value larger than ``0``.

:func:`~pypey.Pype.take` is lazy when it's parameter is positive (head) and eager when it's negative (tail).

In order to stop a lazy backing ``Iterable`` from being consumed, :func:`~pypey.Pype.clone` can be used to ensure that
the consumption happens on the returned pipe and not the original one. :func:`~pypey.Pype.clone` manages this by
using ``iteratools.tee`` under the hood and it's the only method that mutates the internal state of a :class:`~pypey.Pype`

If reading the whole pipe into memory is not an issue, it's often simpler and more efficient to create a pipe with an
eager backing ``Iterable`` (``list``, ``tuple`` and so on). For instance if reading from a file:

.. code-block:: python

    >>> from pypey import pype
    >>> eager_pype = pype.file('text.txt').to(list, Pype)

or more conveniently:

.. code-block:: python

    >>> from pypey import pype
    >>> eager_pype = pype.file('text.txt').eager()

:class:`~pypey.Pype`'s methods can also be categorised according to what kind of pipe they return: *mappings* return a pipe
of the same size as the one they are applied to; *partitions* return divisions of the original pipe; and *selections*
return a subset of the pipe's items:

+---------------+----------------------------------+
| **Mappings**  | :func:`~pypey.Pype.accum`        |
|               | :func:`~pypey.Pype.clone`        |
|               | :func:`~pypey.Pype.enum`         |
|               | :func:`~pypey.Pype.map`          |
|               | :func:`~pypey.Pype.pick`         |
|               | :func:`~pypey.Pype.reverse`      |
|               | :func:`~pypey.Pype.shuffle`      |
|               | :func:`~pypey.Pype.sort`         |
|               | :func:`~pypey.Pype.zip_with`     |
+---------------+----------------------------------+
|**Partitions** | :func:`~pypey.Pype.chunk`        |
|               | :func:`~pypey.Pype.dist`         |
|               | :func:`~pypey.Pype.divide`       |
|               | :func:`~pypey.Pype.group_by`     |
|               | :func:`~pypey.Pype.partition`    |
|               | :func:`~pypey.Pype.split`        |
|               | :func:`~pypey.Pype.unzip`        |
|               | :func:`~pypey.Pype.window`       |
|               | :func:`~pypey.Pype.zip`          |
+---------------+----------------------------------+
|**Selections** | :func:`~pypey.Pype.drop`         |
|               | :func:`~pypey.Pype.drop_while`   |
|               | :func:`~pypey.Pype.reject`       |
|               | :func:`~pypey.Pype.sample`       |
|               | :func:`~pypey.Pype.select`       |
|               | :func:`~pypey.Pype.slice`        |
|               | :func:`~pypey.Pype.take`         |
|               | :func:`~pypey.Pype.take_while`   |
|               | :func:`~pypey.Pype.top`          |
|               | :func:`~pypey.Pype.uniq`         |
+---------------+----------------------------------+

If given an initial value, :func:`~pypey.Pype.accum` will return a pipe with one more item than the original pipe's.

Some methods are just specialisations, or convenient versions of others:

+----------------------------------+----------------------------------+
| General                          | Specific                         |
+==================================+==================================+
| :func:`~pypey.Pype.__iter__`     | :func:`~pypey.Pype.it`           |
+----------------------------------+----------------------------------+
| :func:`~pypey.Pype.do`           | :func:`~pypey.Pype.print`        |
|                                  | :func:`~pypey.Pype.to_file`      |
+----------------------------------+----------------------------------+
| :func:`~pypey.Pype.map`          | :func:`~pypey.Pype.pick`         |
|                                  | :func:`~pypey.Pype.zip_with`     |
+----------------------------------+----------------------------------+
| :func:`~pypey.Pype.sample`       | :func:`~pypey.Pype.shuffle`      |
+----------------------------------+----------------------------------+
| :func:`~pypey.Pype.select`       | :func:`~pypey.Pype.reject`       |
+----------------------------------+----------------------------------+
| :func:`~pypey.Pype.slice`        | :func:`~pypey.Pype.drop`         |
|                                  | :func:`~pypey.Pype.take`         |
+----------------------------------+----------------------------------+
| :func:`~pypey.Pype.zip`          | :func:`~pypey.Pype.enum`         |
+----------------------------------+----------------------------------+
| :func:`~pypey.Pype.window`       | :func:`~pypey.Pype.chunk`        |
+----------------------------------+----------------------------------+

Module contents
---------------

.. automodule:: pypey
   :members:
   :undoc-members:
   :show-inheritance:

.. autodata:: pypey.pypes.pype
.. autodata:: pypey.func.Fn


