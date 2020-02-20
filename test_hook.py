import numpy as np

file = "test.out"


a = np.arange(9)


np.savetxt(file, a, fmt="%d")

if True:
    print("True")
