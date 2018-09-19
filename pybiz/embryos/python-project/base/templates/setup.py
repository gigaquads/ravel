#!/usr/bin/env python3
# encoding=utf-8

import os

from appyratus.util import RealSetup

setup = RealSetup(
    path=os.path.abspath(os.path.dirname(__file__)),
    name='{{ name|snake }}',
    version='{{ version }}',
    description='{{ description }}',
    author='{{ author }}',
    author_email='{{ author_email }}',
    url='{{ url }}',
    classifiers=['python3']
)
setup.run()
