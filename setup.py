#!/usr/bin/env python

from distutils.core import setup

setup(
    name='dbproc',
    version='3.11',
    description='Mappings for easy access to (database) stored procedures in Python',
    author='Wijnand Modderman-Lenstra',
    author_email='maze@pyth0n.org',
    url='https://github.com/tehmaze/dbproc/',
    packages=[
        'dbproc',
        'dbproc/backend',
    ],
)
