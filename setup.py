import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="magical_sqlserver",
    version="0.4.0",
    author="Brenno Flavio de Almeida",
    author_email="brenno.flavio412@gmail.com",
    description="Microsoft SQL Server for applications, like magic",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/brennoflavio/magical-sqlserver",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    install_requires=["pymssql", ],
)
