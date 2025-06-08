from setuptools import setup, find_packages

setup(
    name="pulpo",
    version="1.3.0",
    packages=find_packages(),
    install_requires=[
        "requests",  # Agrega aquí tus dependencias
        "pyjwt"
    ],
)