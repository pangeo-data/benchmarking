from setuptools import find_packages, setup

with open('README.md', 'r') as fh:
    long_description = fh.read()

setup(
    name='pangeobench',
    scripts=['pangeobench'],
    author='Haiying Xu',
    author_email='haiyingx@ucar.edu',
    description='Pangeo zarr benchmarking package',
    long_description=long_description,
    long_description_content_type='text/markdown',
    use_scm_version={'version_scheme': 'post-release', 'local_scheme': 'dirty-tag'},
    install_requires=['dask>=2.11', 'xarray>=0.14', 'numpy>1.17'],
    license='Apache 2.0',
    url='https://github.com/pangeo-data/benchmarking',
    packages=find_packages(),
    classifiers=[
        'Development Status :: 0 - Beta',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: OS Independent',
        'Intended Audience :: Science/Research',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Topic :: Scientific/Engineering',
    ],
)
