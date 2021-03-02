Introduction
============

Pypey is a library for concisely composing data transformation primitives. It support lazily evaluated
`collection pipelines <https://martinfowler.com/articles/collection-pipeline>`_ with standard operations like
`map <https://en.wikipedia.org/wiki/Map_(higher-order_function)>`_, `reduce <https://en.wikipedia.org/wiki/Fold_(higher-order_function)>`_
, `filter <https://en.wikipedia.org/wiki/Filter_(higher-order_function)>`_ and several others others. It is fashioned after libraries like `Java Streams <https://docs.oracle.com/javase/8/docs/api/java/util/stream/package-summary.html>`_ ,
`Immutable.js <https://immutable-js.github.io/immutable-js/docs/#/Seq>`_ and `C++ Streams <http://jscheiny.github.io/Streams>`_.
It has been inspired by, and leans on, the excellent `itertools <https://docs.python.org/3/library/itertools.html>`_  and
`more-itertools <https://github.com/more-itertools/more-itertools>`_

Motivation
----------

Many operations on data, like reading lines from a file, filtering or aggregating items, appear repeatedly across many
domains and so they benefit from encapsulating to avoid duplication and enable code reuse. This encapsulation also has a
second benefit: the abstracted operations now map 1-to-1 to the high-level description, ie, the intent of the coder. A
third advantage is that it allows the different operations in a pipeline to be disentangled from each other.

Let's illustrate all this with an example: a frequent data processing routine where you need to build a word-to-id
dictionary from a file containing words of text. The high-level recipe would be something like this:

.. code-block:: Twig

    1. read lines from file
    2. split lines into words
    3. keep only unique words across all lines
    4. assign a unique number id to each word
    5. put words and their ids in a dictionary

A typical implementation using only python built-ins would look like this:

.. code-block:: python

    with open('text.txt') as file:          # 1. open file for reading
        idx = 0                             # 2. make id counter
        word_to_id = {}                     # 3. make empty dict

        for line in file:                   # 4. loop through lines
            for word in line.split():       # 5. split line and loop through words
                word = word.rstrip()        # 6. strip line terminator
                if word not in word_to_id:  # 7. check to see if it's in dictionary
                    word_to_id[word] = idx  # 8. insert word with a new id if it's not
                    idx += 1                # 9. update id counter



Notice how there are steps ``2.`` , ``3.`` , ``6.``  and ``9.`` do not correspond to anything in the high level recipe.
Notice also, how operations ``4.`` to ``9.`` interleave with each other, happening once per loop iteration. Let's see how
this could be implemented with ``itertools``:

.. code-block:: python

    from itertools import chain, count

    with open('text.txt') as file:              # 1. open file for reading

        lines = file.readlines()                # 2. read lines from file
        stripped = map(str.rstrip, lines)       # 3. strip line terminator
        words = map(str.split, stripped)        # 4. split lines into words
        all_words = chain.from_iterable(words)  # 5. concatenate all lines
        unique = set(all_words)                 # 6. keep only unique words across all lines
        words_ids = zip(unique, count())        # 7. assign a unique id to each word
        word_to_id = dict(words_ids)            # 8. put in dictionary

Now the steps match the original intent better because they operate at higher level, ie, they work at the level of whole
collections of items, and do not concern themselves with the data and syntactic structures needed when the
algorithm is specified at the level of the individual items, as in the first implementation. The next implementation is
more concise and leverages the ability to pipe the collections together:

.. code-block:: python

    from itertools import chain, count

    with open('text.txt') as file:  # 1.
         # 2. + 3. + 4. + 5. + 6. + 7. + 8.
         word_to_id = dict(zip(set(chain.from_iterable(map(str.split, map(str.rstrip, file.readlines())))), count()))

However, the sequence of steps is now laid out in reverse order or "inside-out". With Pypey, the code is still concise
and the steps always flow right:

.. code-block:: python

    from pypey import pype

    word_to_id = (pype.file('text.txt') # 1. read lines from file, strip line terminator by default
                  .map(str.split)       # 2. split lines into words
                  .flat()               # 3. concatenate all lines (by "flattening" them)
                  .uniq()               # 4. keep only unique words across all lines
                  .enum(swap=True)      # 5. assign a unique id to each word
                  .to(dict))            # 6. put in a dictionary

This implementation matches the original intent best and removes the need for the coder to write boiler-plate that
is not domain-specific. A more terse implementation helps when using the Python interpreter's interactive mode (REPL):

.. code-block:: python

    >>> from pypey import pype
    >>> # 1. + 2. + 3. + 4. + 5. + 6.
    >>> word_to_id = pype.file('text.txt').map(str.split).flat().uniq().enum(swap=True).to(dict)

Lazy and Deferred Evaluation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Both ``itertools``-'s and Pypey's implementation would incur a performance penalty if each step created an intermediate
collection. However by piping through lazy collections, ie, those that are evaluated incrementally only one item at
a time as they are iterated through (based on generators), the performance is similar to a loop-based implementation.
Furthermore, just as the loop-based approach, items are only read one at a time into memory, avoiding unnecessary
allocation.

Not all operations can be implemented lazily, for instance, sorting is necessarily "eager" as it entails traversing the
whole collection before being able to retrieve the first sorted item. Pypey still makes these eager operations deferred
to allow delaying the consumption of the lazy collection until it's actually needed:

.. code-block:: python

    >>> p = pype(['a', 'fun', 'day']).sort()
    >>> p
    <pypey.pype.Pype object at 0x7f58edaf4970>
    >>> list(p)
    ['a', 'day', 'fun']

Argument Unpacking
~~~~~~~~~~~~~~~~~~
`PEP 3113 <https://www.python.org/dev/peps/pep-3113>`_ removed Python 2's ability to unpack function arguments from
Python 3. This made using higher-order functions (functions taking or returning other functions) harder when applied to
iterable items in a collection, especially so when lambdas are passed in, as it's impossible to use unpacking assignments
in them. Pypey brings back a limited form of argument unpacking that works only at the top level of nesting. For instance:

.. code-block:: python

    >>> pype.dict({'a':1, 'fun':2, 'day':3}).map(lambda kv: (kv[0], kv[1] + 1)).to(list)
    [('a', 2), ('fun', 3), ('day', 4)]

can also be written more clearly as:

.. code-block:: python

    >>> pype.dict({'a':1, 'fun':2, 'day':3}).map(lambda k, v: (k, v + 1)).to(list)
    [('a', 2), ('fun', 3), ('day', 4)]

Getting Started
---------------
To get started, install the library with pip:

.. code-block:: Shell

    pip install pypey

Then use as:

.. code-block:: python

    >>> from pypey import pype
    >>> pype(range(-2, 3)).map(abs).print(now=True)
    2
    1
    0
    1
    2
    <pypey.pype.Pype object at 0x7f56401e0f40>


To run tests install ``pytest``:

.. code-block:: Shell

    pip install pytest

then run:

.. code-block:: Shell

    pytest




Related Libraries
-----------------

Pypey is similar to `itertools <https://docs.python.org/3/library/itertools.html>`_ and `more-itertools <https://github.com/more-itertools/more-itertools>`_
but takes an object-based approach instead, with method-chaining as the main pipe-building mechanism, instead of
function composition. This allows pipes to always flow right (or down, if properly formatted) which is arguably a more
intuitive ordering for coders used to Object Orientated Programming.

Pypey is perhaps most similar to `python_lazy_streams <https://github.com/brettschneider/python_lazy_streams>`_ as it too
uses an object-based+method-chaining approach. Related is `pipes <https://github.com/robinhilliard/pipes>`_, which
implements function-chaining with operator overloading and decorators, allowing it to compose right like Pypey.
`stream.py <https://github.com/aht/stream.py/blob/master/example/randwalk.py>`_ also uses operator overloading and function chaining.

There's a number of libraries with extensive pipeline APIs, such as `ReactiveX <http://reactivex.io>`_-'s function-chaining
`RxPy <https://rxpy.readthedocs.io/en/latest/get_started.html>`_ , but geared towards real-time event streams.
Nvidia's `Streamz <https://streamz.readthedocs.io/en/latest>`_ is in the same space, but is object-based with method-chaining,
and adds support for `Pandas <https://github.com/pandas-dev/pandas>`_ and `cuDF <https://docs.rapids.ai/api/cudf/stable>`_.
`Apache Beam <https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/wordcount_minimal.py>`_ combines batch- and stream-processing and supports
different backends like `Spark <http://spark.apache.org>`_, and uses function-chaining with operator overloading.

`Riko <https://github.com/nerevu/riko/blob/master/docs/FAQ.rst#what-pipes-are-available>`_ is also an object-based+method-chaining
type of API but specialises in structured text processing.

`Mario <https://github.com/python-mario/mario>`_ is a CLI-based function-chaining API, similar to Unix Shell's pipes.