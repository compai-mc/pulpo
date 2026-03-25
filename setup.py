from setuptools import setup, find_packages

setup(
    name="pulpo",
    version="5.4.1",
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