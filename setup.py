import os
from setuptools import setup, find_packages

here = os.path.abspath(os.path.dirname(__file__))
README = open(os.path.join(here, 'README.rst')).read()
CHANGES = open(os.path.join(here, 'CHANGES.txt')).read()

setup(
    name='tableau',
    version='0.0.2',
    author='Moriyoshi Koizumi',
    author_email='mozo@mozo.jp',
    description="Tableau is a collection of helper classes for building test fixtures and seed data",
    long_description=README + '\n\n' + CHANGES,
    classifiers=[
        'Environment :: Other Environment',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Software Development :: Testing',
        'Topic :: Software Development :: Quality Assurance',
        'Topic :: Utilities'
        ],
    license='MIT License',
    keywords='test testing fixture seed',
    url='http://github.com/moriyoshi/tableau',
    test_suite='tableau.tests',
    tests_require=['sqlalchemy >= 0.7'],
    packages=find_packages()
    )
