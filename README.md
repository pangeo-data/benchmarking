# Benchmarking

Benchmarking &amp; Scaling Studies of the Pangeo Platform

- [Benchmarking](#benchmarking)
  - [Creating an Environment](#creating-an-environment)
  - [Benchmark Configuration](#benchmark-configuration)
  - [Running the Benchmarks](#running-the-benchmarks)
  - [Benchmark Results](#benchmark-results)
  - [Visualization](#visualization)

## Creating an Environment

To run the benchmarks, it's recommended to create a dedicated conda environment by running:

```bash
conda env create -f ./binder/environment.yml
```

This will create a conda environment named `pangeo-bench` with all of the required packages.

You can activate the environment with:

```bash
conda activate pangeo-bench
```

and then run the post build script:

```bash
./binder/postBuild
```

## Benchmark Configuration

The `benchmark-configs` directory contains YAML files that are used to run benchmarks on different machines. So far, the following HPC systems' configs are provided:

```bash
$ tree ./benchmark-configs/
benchmark-configs/
├── cheyenne.yaml
└── hal.yaml
└── wrangler.yaml

```

In case you are interested in running the benchmarks on another system, you will need to create a new YAML file for your system with the right configurations. See the existing config files for reference.

## Running the Benchmarks
### from command line

To run the benchmarks, a command utility `pangeobench` is provided in this repository.
To use it, you simply need to specify the location of the benchmark configuration file. For example:

```bash
./pangeobench run benchmark-configs/cheyenne.readwrite.yaml  #running a weak scaling analysis
./pangeobench run benchmark-configs/cheyenne.write.yaml      #running a strong scaling analysis
./pangeobench run benchmark-configs/cheyenne.read.yaml
```

```bash
$ ./pangeobench --help
Usage: pangeobench [OPTIONS] COMMAND [ARGS]...

Options:
  --help  Show this message and exit.

Commands:
  run     Run benchmarking
  upload  Upload benchmarking files from local directory to S3 object store
```
```bash
./pangeobench run --help
Usage: pangeobench run [OPTIONS] CONFIG_FILE

  Run benchmarking

Options:
  --help  Show this message and exit.
```
```bash
./pangeobench upload --help
Usage: pangeobench upload [OPTIONS]

  Upload benchmarking files from local directory to S3 object store

Options:
  --local_dir PATH     Local directory to upload from  [default: test_write]
  --bucket TEXT        Bucket and directory name of S3 object store  [default:
                       pangeo-bench-local/test_write]

  --profile TEXT       Profile for accessing S3 object store  [default:
                       default]

  --endpoint_url TEXT  Endpoint url of S3 object store  [default:
                       https://***.ucar.edu]

  --config_file PATH   Config file includes all other options, if not
                       provided, you have to specify all other options from
                       command lines

  --help               Show this message and exit.
```
## Running the Benchmarks
### from jupyter notebook.

To run the benchmarks from jupyter notebook, install 'pangeo-bench' kernel to your jupyter notebook enviroment, then start run.ipynb notebook.  You will need to specify the configuration file as described above in your notebook.

To install your 'pangeo-bench' kernel to your jupyter notebook enviroment you'll need to connect a terminal of your HPC enviroment and run following command.

```conda env create -f pangeo-bench.yml
source activate pangeo-bench
ipython kernel install --user --name pangeo-bench
```

Before starting your jupyternotebook, you can verify that if your kernel is well installed or not by follwing command

```
jupyter kernelspec list
```



## Benchmark Results

Benchmark results are persisted in the `results` directory by default. The exact location of the benchmark results depends on the machine name (specified in the config file) and the date on which the benchmarks were run. For instance, if the benchmarks were run on Cheyenne supercomputer on 2019-09-07, the results would be saved in: `results/cheyenne/2019-09-07/` directory. The file name follows this template: `compute_study_YYYY-MM-DD_HH-MM-SS.csv`

## Visualization

Visualisation can be done using jupyter notebooks placed in analysis directories.
