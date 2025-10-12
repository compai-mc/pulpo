from setuptools import setup, find_packages

setup(
    name="pulpo",
    version="3.1.1",
    packages=find_packages(),
    install_requires=[
        "requests", 
        "pyjwt",
        "python-arango",
        "kafka-python"
    ],
)