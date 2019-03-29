from setuptools import setup

setup(
    name='dg-integration',
    version='0.0.2',
    author='Alexander Kass',
    description='Implementation of DBAPI v2 on the top of DataGrip',
    license='MIT',
    packages=['intellij', 'intellij.dgapi'],
    install_requires=[
       'ij-discoverer', 'sqlalchemy', 'python-dateutil'
    ],
    keywords='IntelliJ IDEA DataGrip PhpStorm PyCharm GoLand'
)
