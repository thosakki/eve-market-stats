from datetime import datetime
import unittest

import lib
Order = lib.Order

# Show full diff in unittest
unittest.util._MAX_LENGTH=2000

class TestReadOrderset(unittest.TestCase):
    def runTest(self):
        d = [x for x in lib.read_orderset("testdata/orderset.csv.gz")]
        orders, ordersets = list(zip(*d))
        for o in ordersets[:-2]: self.assertEqual(o, 100177)
        self.assertEqual(orders[0], Order(1109, 60013330, False, 199680.0, 1, datetime.fromisoformat("2022-02-21T11:02:58")))
        self.assertEqual(orders[1], Order(1109, 60013333, False, 199680.0, 1, datetime.fromisoformat("2022-03-29T11:07:07")))
        self.assertEqual(orders[2], Order(1109, 60013336, False, 199680.0, 1, datetime.fromisoformat("2022-02-15T11:06:35")))
        self.assertEqual(orders[3], Order(1109, 60013339, False, 199680.0, 1, datetime.fromisoformat("2022-04-28T11:02:59")))


unittest.main()
