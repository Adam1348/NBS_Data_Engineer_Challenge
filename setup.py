try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

config = {
    'description': 'my_challenge',
    'author': 'Zhi Wang',
    'author_email': 'zw1348@nyu.edu',
    'version': '0.1',
    'install_requires': [],
    'packages': ['my_challenge'],
    'name': 'Data-Engineer-Challenge'
}

setup(**config)