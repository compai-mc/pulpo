from setuptools import setup, find_packages

setup(
    name="pulpo",
    version="5.0.1",
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