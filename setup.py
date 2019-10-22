import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()


requires = [
    "boto3==1.9.200",
    "ipython==7.6.1",
    "openpyxl",
    "pandas==0.25.1",
    "SQLAlchemy==1.3.5",
    "sqlparse==0.3.0",
    "simple-salesforce==0.74.2",
    "dask==2.1.0",
    "exchangelib==2.0.1",
    "pendulum==2.0.5",
    "croniter==0.3.30",
    "graphviz"
]

setuptools.setup(
    name="grizly",
    version="0.0.1",
    author="Alessio Civitillo",
    description="Small package to build SQL with a Pandas api",
    long_description=long_description,
    long_description_content_type="text/markdown",
    #url="https://github.com/pypa/sampleproject",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    install_requires=requires
)