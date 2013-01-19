from setuptools import setup

setup(name='beanstalktc',
      version='0.1.0',
      description='An async beanstalkd client for Tornado',
      author='Jacob Sondergaard',
      author_email='jacob@nephics.com',
      license="http://www.apache.org/licenses/LICENSE-2.0",
      url='https://bitbucket.org/nephics/beanstalktc',
      packages=['beanstalktc'],
      requires=['tornado(>=2.4)']
)