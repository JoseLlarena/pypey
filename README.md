# Pypey

Library for composing lazily-evaluated data-processing pipelines:

```python

>>> from urllib.request import urlopen
>>> import re
>>> from pypey import pype, px
>>> # prints plain text message from example.com
>>> (pype(urlopen('http://example.com'))
...  .map(bytes.decode, str.split)
...  .flat()
...  .slice(90, 114)
...  .map(px(re.sub, r'\<\/?p\>', ''))
...  .to(' '.join))
'This domain is for use in illustrative examples in documents. You may use this domain in literature without prior coordination or asking for permission.'
```
Supports argument unpacking:

```python
>>> import os
>>> from pypey import pype
>> # reads environment variables with "PYTHON" in them and prints their name
>>> pype.dict(os.environ).select(lambda key, val: 'PYTHON' in key).pick(0).print(now=True)
PYTHONPATH
IPYTHONENABLE
PYTHONIOENCODING
PYTHONUNBUFFERED
<pypey.pype.Pype object at 0x7ffa54006a30>
```
Supports parallelism:

```python
>>> from timeit import timeit
>>> timeit("from math import log10; from pypey import pype; pype(range(1, 10_000_000)).map(log10).to(sum, print)", number=5)
65657052.08005966
65657052.08005966
65657052.08005966
65657052.08005966
65657052.08005966
10.055954356997972
>>> timeit("from math import log10; from pypey import pype; pype(range(1, 10_000_000)).map(log10, workers=8).to(sum, print)", number=5)
65657052.08005966
65657052.08005966
65657052.08005966
65657052.08005966
65657052.08005966
5.446932412014576
```

## Installing

Install with pip:

```shell
pip install pypey
```

## Testing

Unit tests are written with  [pytest](https://docs.pytest.org/en/stable). Run with:

```shell
pip install pytest

pytest
```
Pypey has been tested with python 3.7, 3.8 and 3.9. A [few tests](https://github.com/JoseLlarena/pypey/blob/master/unittests/test_unpacking_of_arguments.py)
containing [positional-only arguments](https://www.python.org/dev/peps/pep-0570) will fail if they are run with 3.7 as 
it's a feature only available from 3.8 onwards.

## Documentation

Extensive documentation at https://pypey.readthedocs.io/en/stable/index.html. [Unit tests](https://github.com/JoseLlarena/pypey/tree/master/unittests) 
document through behaviour specification.

## Changelog

Check the [Changelog](https://github.com/JoseLlarena/pypey/blob/master/CHANGELOG.md) for fixes and enhancements of each version.

## License

Copyright Jose Llarena 2021.

Distributed under the terms of the [MIT](https://github.com/JoseLlarena/pypey/blob/master/LICENSE) license, Pypey is free 
and open source software.