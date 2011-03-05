import random
import time

from twisted.trial import unittest


def seed(value):
    def decorator(func):
        func._random_seed = value
        return func
    return decorator


class TestCase(unittest.TestCase):
    def _run(self, method_name, result):
        method = getattr(self, method_name)
        seed = getattr(method, '_random_seed', int(time.time()))
        random.seed(seed)

        def seed_info_adder(failure):
            failure.value.args = (failure.value.args[0] + ' (random seed: %s)' % seed,) + failure.value.args[1:]
            return failure

        d = super(TestCase, self)._run(method_name, result)
        d.addErrback(seed_info_adder)
        return d

