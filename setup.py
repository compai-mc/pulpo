from setuptools import setup, find_packages

setup(
    name="pulpo",
    version="5.1.4",
    packages=find_packages(),
    install_requires=[
        "requests", 
        "pyjwt",
        "python-arango",
        "kafka-python",
        "langroid",
        "hvac",
        "json5"
    ],
)