from setuptools import find_packages, setup

setup(
    name="reportseff",
    version=0.3,
    author="Troy Comi",
    description="Tablular seff output",
    url="https://github.com/troycomi/reportseff",
    packages=find_packages(),
    install_requires=["Click"],
    entry_points="""
        [console_scripts]
        reportseff=reportseff.reportseff:reportseff
""",
)
