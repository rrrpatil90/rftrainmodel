Random Forest Training Model
----------------------------

Install "coverage" package to run test coverage. It is added in requirement.txt file.
pip install coverage

To run test cases, simply do::

python rftrainmodel_test.py


MAINFEST.in file is necessary to tell setuptools to include the README.rst file when generating source 
distributions. Otherwise, only Python files will be included.

When the repo is hosted on SVN, the README.rst file will also automatically be picked up 
and used as a ‘homepage’ for the project.