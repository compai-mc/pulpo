from setuptools import setup, find_packages

setup(
    name="pulpo",
<<<<<<< Updated upstream
    version="5.7.1",
=======
    version="5.7.9",
>>>>>>> Stashed changes
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
