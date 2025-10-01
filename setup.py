from setuptools import setup, find_packages

setup(
    name="pulpo",
    version="2.0.19",
    packages=find_packages(),
    install_requires=[
        "requests", 
        "pyjwt",
        "aiokafka",
        "python-arango"
    ],
)