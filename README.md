# Python Utilities

![Python Utilities](doc/robot-working.jpg)

This module is a collection of miscellaneous python utilities used by several ML python  projects which share these functions. Therefore caution should be excercised when modifying this code as it could affect several other repositories.

The functions in this library have been partitioned into the following categories:

- File system utilities
- Data manipulation
- I/O Abstraction API
- GCP Python API
- Neo4j Python API
- OpenAI Python API
- Common Queries
- NLP utilities

An API guide for this library will be provided soon.

----

## Installation and Environment Configuration

[Installation](doc/installation.md)

----

## Usage in Other Modules

While this module can be used as a standalone package, it was intented to be used as a common library across several Python projects.

This Python library can be specified in a requirements.txt file as:

    git+ssh://git@github.com/delaray/pytils.git

or can be installed at the command with:

	pip install --ignore-installed git+ssh://git@github.com/delaray/pytils.git@develop

-----
