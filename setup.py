import codecs
import os.path
import re

from setuptools import setup


here = os.path.abspath(os.path.dirname(__file__))

# Read the version number from a source file.
def find_version(*file_paths):
    # Open in Latin-1 so that we avoid encoding errors.
    # Use codecs.open for Python 2 compatibility
    with codecs.open(os.path.join(here, *file_paths), 'r', 'latin1') as f:
        version_file = f.read()

    # The version line must have the form
    # __version__ = 'ver'
    version_match = re.search(r"^__version__ = ['\"]([^'\"]*)['\"]",
                              version_file, re.M)
    if version_match:
        return version_match.group(1)
    raise RuntimeError("Unable to find version string.")


version = find_version('beanstalkt', 'beanstalkt.py')

setup(
    name='beanstalkt',
    version=version,
    description='An async beanstalkd client for Tornado',
    author='Jacob Sondergaard',
    author_email='jacob@nephics.com',
    license="http://www.apache.org/licenses/LICENSE-2.0",
    url='https://bitbucket.org/nephics/beanstalkt',
    packages=['beanstalkt'],
    requires=['tornado(>=3.2)'],
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3'
    ],
    download_url='https://bitbucket.org/nephics/beanstalkt/get/v{}.tar.gz'
                 .format(version)
)