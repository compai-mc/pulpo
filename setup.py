from setuptools import setup, find_packages

setup(
    name="pulpo",
    version="5.7.9",
main
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
