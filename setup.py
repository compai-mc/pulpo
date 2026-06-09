from setuptools import setup, find_packages

setup(
    name="pulpo",
    version="5.9.1",
    packages=find_packages(),
    install_requires=[
        "requests",
        "python-arango",
        "kafka-python",
        "langroid",
        "hvac",
        "httpx",
        "json5",
        "PyJWT"
    ],
)
