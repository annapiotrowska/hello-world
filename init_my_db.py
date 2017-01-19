# -*- coding: utf-8 -*-
#tworzenie tabel w bazie
import sqlite3

db_path = 'deals.db'
conn = sqlite3.connect(db_path)

c = conn.cursor()
#
# Tabele
#
c.execute('''
          CREATE TABLE Deals
          ( id INTEGER PRIMARY KEY,
            deal_date DATE,
            deal_side VARCHAR(5),
            qty INTEGER,
            notional_amount NUMERIC
          )
          ''')
c.execute('''
          CREATE TABLE Deals_Positions
          ( position INTEGER PRIMARY KEY,
            indeks_name VARCHAR(100),
            price NUMERIC,
            qty INTEGER,
            amount NUMERIC,
            pricing_beg_date DATE,
            pricing_end_date DATE,
            deal_id INTEGER,
           FOREIGN KEY(deal_id) REFERENCES Deals(id) ON DELETE CASCADE)
          ''')
