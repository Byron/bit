The bit project is a collection of libraries and tools to aid maintaining a companies IT infrastructure.

## Features

- configurable plugin-based filesystem monitor to run automation based on filesystem changes
- zfs library to maintain a database of multiple zfs hosts and direct snapshot exchange between them
- a tool to maintain and mine filesystem metadata with high performance

## Requirements

* [bcore](https://github.com/Byron/bcore)
* [sqlalchemy](https://github.com/zzzeek/sqlalchemy)
    - For some tools to be useful in production, an sql database server is required.
    - For local testing, sqlite databases can be used without problem
* ([lz4](https://pypi.python.org/pypi/lz4))
    - Only used if available to compute entrophy of files when gathering filesystem statistics

Optionally, you may need the following

* nosetests
    -  Developers use it to run unit tests to verify the program works as expected

## Development Status

[![Coverage Status](https://coveralls.io/repos/Byron/bit/badge.png)](https://coveralls.io/r/Byron/bit)
[![Build Status](https://travis-ci.org/Byron/bit.svg?branch=master)](https://travis-ci.org/Byron/bit)
![under construction](https://raw.githubusercontent.com/Byron/bcore/master/src/images/wip.png)

### LICENSE

This open source software is licensed under [GNU Lesser General Public License](https://github.com/Byron/bit/blob/master/LICENSE.md)
