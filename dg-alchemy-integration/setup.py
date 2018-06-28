from setuptools import setup

entry_points = {
    'sqlalchemy.dialects': [
        'access = intellij.alchemy.dgapi:AccessDialect_dgapi',
        'access.pyodbc = intellij.alchemy.dgapi:AccessDialect_dgapi',
    ]
}
setup(
    name='dg-alchemy-integration',
    version='0.0.1',
    author='Alexander Kass',
    description='Implementation of SQLAlchemy API for DataGrip connection',
    license='MIT',
    packages=['intellij', 'intellij.alchemy'],
    install_requires=[
        'dg-integration',
    ],
    keywords='IntelliJ IDEA DataGrip PhpStorm PyCharm GoLand'
)
