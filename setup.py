#!/usr/bin/env python3
# encoding=utf-8
# vim: set filetype=python
import os

from appyratus.util import RealSetup

setup = RealSetup(
    path=os.path.abspath(os.path.dirname(__file__)),
    name='pybiz',
    version='0b0',
    description='Pybiz',
    author='Gigaquads',
    author_email='notdsk@gmail.com',
    url='https://github.com/gigaquads/pybiz',
    classifiers=['python3']
)
setup.run()
