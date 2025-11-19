from setuptools import setup, find_packages

setup(
    name="pulpo",
    version="4.9.0",
    packages=find_packages(),
    install_requires=[
        "requests", 
        "pyjwt",
        "python-arango",
        "kafka-python",
        "langroid",
        "hvac"
    ],
)