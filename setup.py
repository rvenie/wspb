from setuptools import find_packages, setup

setup(
    name="buildings_pipeline",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "dagster",
        "dagster-webserver",
        "pandas",
        "requests",
        "beautifulsoup4",
        "openpyxl",
        "python-dotenv",
    ],
    extras_require={
        "dev": [
            "pytest",
        ],
    },
)
