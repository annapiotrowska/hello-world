# -*- coding: utf-8 -*-

import repository #nazwa pliku która zawiera repozytorium
import sqlite3
import unittest

db_path = 'deals.db'

class RepositoryTest(unittest.TestCase):

    def setUp(self):
        conn = sqlite3.connect(db_path)
        c = conn.cursor()
        c.execute('DELETE FROM Deals_Positions')
        c.execute('DELETE FROM Deals')
        c.execute('''INSERT INTO Deals (id, deal_date, deal_side, qty, notional_amount) VALUES(105, '2016-01-01', 'BUY',15000,7250000)''')
        c.execute('''INSERT INTO Deals_Positions (position, indeks_name, price, qty, amount, pricing_beg_date, pricing_end_date, deal_id) VALUES(20,'JETCIF',480.0,10000,4800000,'2017-01-01','2017-01-31',105)''')
        c.execute('''INSERT INTO Deals_Positions (position, indeks_name, price, qty, amount, pricing_beg_date, pricing_end_date, deal_id) VALUES(21,'JETCIF',490.0,5000,2450000,'2017-02-01','2017-02-28',105)''')
        conn.commit()
        conn.close()

    def tearDown(self):
        conn = sqlite3.connect(db_path)
        c = conn.cursor()
        c.execute('DELETE FROM Deals_Positions')
        c.execute('DELETE FROM Deals')
        conn.commit()
        conn.close()

    def testGetByIdInstance(self):
        deal = repository.DealsRepository().getById(105)
        self.assertIsInstance(deal, repository.Deals, "Objekt nie jest klasy Deals")

    def testGetByIdNotFound(self):
        self.assertEqual(repository.DealsRepository().getById(107),
                None, "Powinno wyjść None")

    def testGetByIdInvitemsLen(self):
        self.assertEqual(len(repository.DealsRepository().getById(105).positions),
                2, "Powinno wyjść 2")

    def testDeleteNotFound(self):
        self.assertRaises(repository.RepositoryException,
                repository.DealsRepository().delete, 107)



if __name__ == "__main__":
    unittest.main()
