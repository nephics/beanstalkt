from setuptools import setup

setup(
    name='beanstalkt',
    version='0.6.0',
    description='An async beanstalkd client for Tornado',
    author='Jacob Sondergaard',
    author_email='jacob@nephics.com',
    license="http://www.apache.org/licenses/LICENSE-2.0",
    url='https://bitbucket.org/nephics/beanstalkt',
    packages=['beanstalkt'],
    requires=['tornado(>=2.4)'],
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3'
    ],
    download_url='https://bitbucket.org/nephics/beanstalkt/get/v0.6.0.tar.gz'
)