from setuptools import setup, find_packages

setup(
    name="moto3",
    version="0.1.4",
    packages=find_packages(),
    install_requires=[
        "boto3",
        "tqdm",
        "tenacity",
        "botocore"
    ],
    author="Matt Deitke",
    author_email="mattd@allenai.org",
    description="An internal custom wrapper around AWS boto3.",
    url="https://github.com/mattdeitke/moto3",
)
