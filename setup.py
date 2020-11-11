from setuptools import setup, find_packages

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name="maggtomic",
    url="https://github.com/polyneme/maggtomic",
    packages=find_packages(),
    use_scm_version=True,
    setup_requires=["setuptools_scm"],
    author="Donny Winston",
    author_email="donny@polyneme.xyz",
    description="Metadata aggregation using the Datomic information model",
    long_description=long_description,
    long_description_content_type="text/markdown",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: BSD License",
    ],
    python_requires=">=3.6",
)
