from setuptools import setup

def readme():
    with open('README.rst') as f:
        return f.read()

setup(name='DMLERandomForestTrainModel',
      version='0.1',
      description='DMLERandomForestTrainModel',
      long_description=readme(),
      url='https://github.com/rrrpatil90/rftrainmodel.git',
      author='Rohit',
      author_email='rohit.patil.consultant@nielsen.com',
      packages=['RFTrainModel'],
      include_package_data=True,
      zip_safe=False)