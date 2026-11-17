from setuptools import setup, find_packages

setup(
    name="pulpo",
    version="5.9.17",
    packages=find_packages(),
    install_requires=[
        "requests",
        "python-arango",
        "kafka-python",
        "hvac",
        "httpx",
        "json5",
        "PyJWT",
        "boto3"
    ],
)
