stormtracks AWS tests go in the tests/ directory and subdirectories. All tests should be run before each new release. Additionally, style checking is done using the pep8 `style checker <https://pypi.python.org/pypi/pep8>`_, with a modified maximum line length of 100 characters.

Tests must be run from the tests/ directory due to the way that the current stormtracks module is loaded (by using ``sys.path.insert(0, '../..')``)

`PEP8 <http://legacy.python.org/dev/peps/pep-0008/>`_ tests with:

::

    nosetests pep8

run all tests:

::

    nosetests 
