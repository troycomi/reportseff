from setuptools import setup, find_packages

setup(
    name='reportseff',
    version=0.2,
    author='Troy Comi',
    description='Tablular seff output',
    url='https://github.com/troycomi/reportseff',
    packages=find_packages(),
    install_requires=[
        'Click',
    ],
    entry_points='''
        [console_scripts]
        reportseff=reportseff.reportseff:reportseff
''',
)
