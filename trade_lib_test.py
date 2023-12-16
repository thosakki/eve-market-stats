import io
import unittest

import trade_lib

class TestGetMostTradedItems(unittest.TestCase):
    def testBasic(self):
        d = io.StringIO("""ID,Name,GroupID,CategoryID,Value Traded,Buy,Sell
2679,Scourge Rage Heavy Assault Missile,654,8,961403854,79.1,90.0
""")
        r = [x for x in trade_lib.get_most_traded_items(d, None)]
        self.assertEqual(r, [trade_lib.ItemSummary(2679, "Scourge Rage Heavy Assault Missile", 654, 8, 961403854, 79.1, 90.0)])

    def testParseFailure(self):
        d = io.StringIO("""ID   Name    GroupID CategoryID
2679    Scourge Rage Heavy Assault Missile  654 8
""")
        with self.assertRaises(RuntimeError) as ctx:
            [x for x in trade_lib.get_most_traded_items(d, None)]


unittest.main()
