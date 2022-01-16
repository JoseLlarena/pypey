from pathlib import Path
from setuptools import setup

setup(name='pypey',
      version='3.0.2',
      description='A library for building data pipelines',
      long_description=(Path(__file__).parent.resolve() / 'README.md').read_text(encoding='utf8'),
      long_description_content_type='text/markdown',
      url='https://github.com/JoseLlarena/pypey',
      author='Jose Llarena',
      author_email='jose.llarena@gmail.com',
      license='MIT',
      classifiers=[
          'Development Status :: 5 - Production/Stable',
          'Environment :: Other Environment',
          'Intended Audience :: Developers',
          'License :: OSI Approved :: MIT License',
          'Natural Language :: English',
          'Operating System :: OS Independent',
          'Programming Language :: Python :: 3',
          'Programming Language :: Python :: 3.7',
          'Programming Language :: Python :: 3.8',
          'Programming Language :: Python :: 3.9',
          'Programming Language :: Python :: 3 :: Only',
          'Programming Language :: Python :: Implementation :: CPython',
          'Topic :: Utilities',
          'Topic :: Software Development :: Libraries'
      ],
      keywords='pipes, lazy collections, streaming, combinators',
      packages=['pypey'],
      python_requires='>=3.7',
      install_requires=['more-itertools', 'pathos'],
      extras_require={'test': ['pytest']})
