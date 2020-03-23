import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()
setuptools.setup(
    name="pangeobench",
    scripts=["pangeobench"],
    author="Haiying Xu",
    author_email="haiyingx@ucar.edu",
    description="Pangeo zarr benchmarking package",
    long_description=long_description,
    long_description_content_type="text/markdown",
    use_scm_version={
        "version_scheme": "post-release",
        "local_scheme": "dirty-tag",
        "write_to": "benchmarks/version.py",
    },
    url="https://github.com/pangeo-data/benchmarking",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
