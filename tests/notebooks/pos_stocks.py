from __future__ import division

import numpy as np
from scipy import stats
import seaborn as sns


def ev(dist):

    """
    :param dist: dictionary representing a probability distribution
    :return: the expected value of this distribution
    """
    assert sum(dist.values()) == 1
    return sum(dist[v] * v for v in dist.keys())


def post_pad(vect, target_size):
    """
    pads the end of this vectors with 0s s.t. it is as long as T_size
    """
    return np.pad(vect, [0, target_size - len(vect)], "constant", constant_values=0)


def pre_pad(vect, n_pads):
    """
    pads the beginning of this vectors with the requested amount of 0s
    """
    return np.pad(vect, [n_pads, 0], "constant", constant_values=0)


def binom_pmf(n, p):
    """
    return a binomial(n,p) pmf
    """

    def _pmf(k):
        return stats.binom.pmf(k, n, p)

    return _pmf


def build_heatmap(transition_matrix, **kwargs):
    """
    convenience method to show a heatmap representing this transition matrix
    """
    return sns.heatmap(transition_matrix,
                       xticklabels=False,
                       yticklabels=False,
                       **kwargs)


def compute_stationary(transition_matrix):
    A = transition_matrix - np.identity(transition_matrix.shape[0])

    # adding one more constraint force x being a probability vector
    prob_const = np.ones([1, transition_matrix.shape[1]])
    A2 = np.concatenate([A, prob_const], axis=0)

    b = np.concatenate([np.zeros([transition_matrix.shape[0], 1]), [[1]]], axis=0)

    x, res, rank, s = np.linalg.lstsq(A2, b)

    return x.T[0], res
