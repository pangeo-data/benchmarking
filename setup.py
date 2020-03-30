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
    use_scm_version={"version_scheme": "post-release", "local_scheme": "dirty-tag"},
    install_requires=["dask>=2.11", "xarray>=0.14", "numpy>1.17"],
    url="https://github.com/pangeo-data/benchmarking",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
