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
    return np.pad(vect, [0, target_size- len(vect)], "constant", constant_values=0)


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
                xticklabels=False, yticklabels=False, **kwargs)


def compute_stationary(transition_matrix):
    A = transition_matrix - np.identity(transition_matrix.shape[0])

    # adding one more constraint force x being a probability vector
    prob_const = np.ones([1, transition_matrix.shape[1]])
    A2 = np.concatenate([A, prob_const], axis=0)

    b = np.concatenate([np.zeros([transition_matrix.shape[0], 1]), [[1]]], axis=0)

    x, res, rank, s = np.linalg.lstsq(A2, b)

    return x.T[0], res


def bounded_sigmoid(x_min, x_max, shape, incrementing=True):
    """
    Builds a S-shape curve that have y values evolving between 0 and 1 over
    the x domain [x_min, x_max]

    This is preferable to the logistic function for cases where we want to
    make sure that the curve actually reaches 0 and 1 at some point (e.g.
    probability of triggering an "restock" action must be 1 if stock is as
    low as 1).

    :param x_min: lower bound of the x domain
    :param x_max: lower bound of the x domain
    :param incrementing: if True, evolve from 0 to 1, or from 1 to 0 otherwise
    :param shape: strictly positive number controlling the shape of the
                  resulting function
                  * 1 correspond to linear transition
                  * higher values yield a more and more sharper, i.e. more
                    vertical S shape, converging towards a step function
                    transiting at (x_max-x_min)/2 for very large values of S (
                    e.g. 10000)
                  * values in ]0,1[ yield vertically shaped sigmoids, sharply
                    rising/falling at the boundary of the x domain and
                    transiting more smoothly in the middle of it.
    """

    def f(x):
        if x < x_min:
            return f(x_min)
        
        if x > x_max: 
            return f(x_max)
        
        if incrementing:
            return stats.beta.cdf((x-x_min)/(x_max-x_min), a=shape, b=shape)
        else:
            return stats.beta.sf((x-x_min)/(x_max-x_min), a=shape, b=shape)
    
    return f
