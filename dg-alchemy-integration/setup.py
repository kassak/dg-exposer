from setuptools import setup

setup(
    name='dg-alchemy-integration',
    version='0.0.1',
    author='Alexander Kass',
    description='Implementation of SQLAlchemy API for DataGrip connection',
    license='MIT',
    packages=['intellij', 'intellij.alchemy'],
    install_requires=[
        'dg-integration', 'sqlalchemy'
    ],
    keywords='IntelliJ IDEA DataGrip PhpStorm PyCharm GoLand',
    entry_points={
        'sqlalchemy.dialects': [
            'dg = intellij.alchemy.dgapi:DynamicDialect_dgapi',
        ]
    }
)
