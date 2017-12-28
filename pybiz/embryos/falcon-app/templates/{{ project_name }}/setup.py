#!/usr/bin/env python

import os
import re

from setuptools import setup, find_packages

if __name__ == '__main__':
    here = os.path.abspath(os.path.dirname(__file__))
    requirements = []
    dependency_links = []

    with open(os.path.join(here, 'readme.md')) as f:
        readme = f.read()

    with open(os.path.join(here, 'requirements.txt')) as f:
        # requirements.txt is formatted for pip install, not setup tools.
        # As a result, we have to manually detect dependencies on github
        # and translate these into data setuptools knows how to handle.
        for line in f.readlines():
            if line.startswith('-e'):
                # we're looking at a github repo dependency, so
                # isntall from a github tarball.
                match = re.search(r'(https://github.+?)#egg=(.+)$',
                                  line.strip())
                url, egg = match.groups()
                if url.endswith('.git'):
                    url = url[:-4]
                tarball_url = url.rstrip('/') + '/tarball/master#egg=' + egg

                dependency_links.append(tarball_url)
                requirements.append(egg)

            else:
                requirements.append(line.strip().replace('-', '_'))

    setup(
        name='{{ name }}',
        version='1.0',
        description='{{ name }}',
        long_description=readme,
        install_requires=requirements,
        dependency_links=dependency_links,
        packages=find_packages(), )
