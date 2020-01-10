import numpy as np


def exponential_sample(alpha=1):
    unif = np.random.uniform(0, 1)
    exp = -np.log(1 - unif) * alpha
    return exp
