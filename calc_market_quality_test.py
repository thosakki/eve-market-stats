import calc_market_quality as calc
import unittest

class TestWeightedMean(unittest.TestCase):
    def testSimple(self):
        self.assertEqual(calc.weighted_mean([(1, 1), (1, 2), (1, 3)]), 2)

    def testWeighted(self):
        self.assertEqual(calc.weighted_mean([(1, 1), (2, 2), (5, 3)]), 2.5)

unittest.main()
