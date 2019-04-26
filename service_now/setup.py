#!/usr/bin/env python

from setuptools import setup

setup(name='assignment_group',
      version='0.0.1',
      description='Load assignment_group',
      author='GGKTECH/NarendraK',
      url='',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['assignment_group'],
      install_requires=['requests', 'pyodbc'],
      entry_points='''
          [console_scripts]
          assignment_group=assignment_group:main
      ''',
      packages=['assignment_group'],
      package_data={
         },
      include_package_data=True
)
