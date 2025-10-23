from setuptools import setup, find_packages

setup(
    name="pulpo",
    version="4.1.5",
    packages=find_packages(),
    install_requires=[
        "requests", 
        "pyjwt",
        "python-arango",
        "kafka-python",
        "langroid"
    ],
)