#!/usr/bin/env python3
# Dfi Helper Tools

from distutils.core import setup

setup(name = 'dfitools',
        version = '0.1.1',
        description = 'Diverse folio isle shared objects and functions',
        author = 'Peder Landsverk',
        author_email = 'pglandsverk@gmail.com',
        url = 'https://github.com/peder2911',
        packages = ['dfitools'],
        requires = ['redis'])


