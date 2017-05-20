import os

from setuptools import setup, find_packages


if __name__ == '__main__':
    HERE = os.path.abspath(os.path.dirname(__file__))

    with open(os.path.join(HERE, 'README.md')) as f:
        README = f.read()

    with open(os.path.join(HERE, 'requirements.txt')) as f:
        REQUIREMENTS = [s.strip().replace('-', '_') for s in f.readlines()]

    setup(name='pybiz',
          version='1.0',
          description='PyBiz',
          long_description=README,
          install_requires=REQUIREMENTS,
          packages=find_packages(),
          )
