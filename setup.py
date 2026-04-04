from setuptools import setup, find_packages

setup(
    name="pulpo",
    version="5.5.0",
    packages=find_packages(),
    install_requires=[
        "requests",
        "python-arango",
        "kafka-python",
        "langroid",
        "hvac",
        "json5",
        "PyJWT"
    ],
)