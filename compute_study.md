# Scaling Study Parameters for Computation

Sample / Fake Data:

- Use fake (2+1)D dataset
- Adjustable resolution in horizontal dimensions and temporal dimension

### Study Machines

- Cheyenne (~60GB memory limit/node)
- Casper (only 22 nodes total)
- Wrangler (???)

Following data assumes running on Cheyenne.

### Computation Operations

Example Operations:

1. Average over horizontal dimensions leaving time dimension (e.g., global mean)
2. Average over time dimension leaving horizontal dimensions (e.g., temporal mean)
3. Reducing (but not elimating) time dimension (e.g., climatology)
4. Groupby subtraction of climatology from original data (e.g., anomaly)

Perform all 4 operations on different data sizes and number of compute nodes.

Plot each operation on its own

### Dask Setup & Cluster Size

- Everything below assumes 1 dask worker per node (on Cheyenne)
- Duplicate each study for 2, 4, 8, and 16 workers per node (reducing chunk size proportionally)
- Single threaded

### Weak Scaling Parameters

Nominal Chunk Size (data size per worker/node): 64MB, 128MB, 256MB, 512MB, 1024MB
Number of Nodes: 1, 2, 4, 8, 16

Corresponding Total Dataset Sizes:
- 64MB Chunk Size:  64MB, 128MB, 256MB, 512MB, 1024MB
- 128MB Chunk Size: 128MB, 256MB, 512MB, 1024MB, 2048MB
- 256MB Chunk Size:  256MB, 512MB, 1024MB, 2048MB, 4096MB
- 512MB Chunk Size:  512MB, 1024MB, 2048MB, 4096MB, 8192MB
- 1024MB Chunk Size:  1024MB, 2048MB, 4096MB, 8192MB, 16384MB

TOTAL Number of Runs: 25 (5 weak scaling curves)

Ideally, this produces 5 horizontal lines.

### Strong Scaling Parameters

Total Dataset Size: 1024MB, 4096MB, 16384MB
Number of Nodes: 1, 2, 4, 8, 16

Corresponding Chunk Sizes:
- 1024MB Total Size: 1024MB, 512MB, 256MB, 128MB, 64MB
- 4096MB Total Size: 4096MB, 2048MB, 1024MB, 512MB, 256MB
- 16384MB Total Size: 16384MB, 8192MB, 4096MB, 2048MB, 1024MB

TOTAL Number of Runs:  15 (3 strong scaling curves)

Ideally, this produces 3 lines that half time when nodes double.

### Framework for Automation

- Run entire suite of jobs using a script and dask-jobqueue
  - Need to wait for resources to be available before proceeding
  - Need to rerun multiple times (to get good statistics)
- Helper function to generate fake data (persisted on workers) given chunk size & number of workers
- Helper functions for each computation operation (acting on data)
