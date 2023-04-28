from setuptools import setup, find_packages

setup(
    name="moto3",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "boto3",
        "tqdm",
    ],
    author="Matt Deitke",
    author_email="mattd@allenai.org",
    description="A custom wrapper around AWS boto3.",
    url="https://github.com/mattdeitke/moto3",
)
