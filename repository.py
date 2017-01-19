# -*- coding: utf-8 -*-

import sqlite3
from datetime import datetime
from datetime import date

#
# Ścieżka połączenia z bazą danych
#
db_path = 'deals.db'

#
# Wyjątek używany w repozytorium
#
class RepositoryException(Exception):
    def __init__(self, message, *errors):
        Exception.__init__(self, message)
        self.errors = errors


#
# Model danych
#
class Deals():
    """Model pojedynczej transakcji
    """
    def __init__(self, id, deal_date=datetime.now(), deal_side="BUY", positions=[]):
        self.id = id
        self.deal_date = deal_date
        self.deal_side = deal_side
        self.positions = positions
        self.qty=sum([item.qty for item in self.positions])
        self.notional_amount = sum([item.price*item.qty for item in self.positions])

    def __repr__(self):  # zamiana obiektu na napis
        return "<Deals(id='%s', deal_date='%s', deal_side='%s', quantity='%s', notional_amount='%s', items='%s')>" % (
                    self.id, self.deal_date, self.deal_side, str(self.qty), str(self.notional_amount), str(self.positions)
                )


class Deals_Positions():
    """Model pozycji danej transakcji. Występuje tylko wewnątrz obiektu Deals.
    """
    def __init__(self, position, indeks_name, price, qty, amount, pricing_beg_date, pricing_end_date):
        self.position=position
        self.indeks_name = indeks_name
        self.price=price
        self.qty = qty
        self.amount = amount
        self.pricing_beg_date=pricing_beg_date
        self.pricing_end_date=pricing_end_date

    def __repr__(self):
        return "<Deals_Positions(position='%s', indeks_name='%s', price='%s', qty='%s', amount='%s', pricing_beg_date='%s', pricing_end_date='%s')>" % (
                    self.position,self.indeks_name, str(self.price), str(self.qty), str(self.amount), str(self.pricing_beg_date), str(self.pricing_end_date)
                )


#
# Klasa bazowa repozytorium
#
class Repository():
    def __init__(self):
        try:
            self.conn = self.get_connection()
        except Exception as e:
            raise RepositoryException('GET CONNECTION:', *e.args)
        self._complete = False

    # wejście do with ... as ...
    def __enter__(self):
        return self

    # wyjście z with ... as ...
    def __exit__(self, type_, value, traceback):
        self.close()

    def complete(self):
        self._complete = True

    def get_connection(self):
        return sqlite3.connect(db_path)

    def close(self):
        if self.conn:
            try:
                if self._complete:
                    self.conn.commit()
                else:
                    self.conn.rollback()
            except Exception as e:
                raise RepositoryException(*e.args)
            finally:
                try:
                    self.conn.close()
                except Exception as e:
                    raise RepositoryException(*e.args)

#
# repozytorium obiektow typu Deals
#
class DealsRepository(Repository):

    def add(self, deal):
        """Metoda dodaje pojedynczą transakcję do bazy danych,
        wraz ze wszystkimi jej pozycjami.
        """
        try:
            c = self.conn.cursor()
            # zapisz nagłowek transakcji

            qty=sum([item.qty for item in deal.positions])
            notional_amount = sum([item.price*item.qty for item in deal.positions])
            c.execute('INSERT INTO Deals (id, deal_date, deal_side, qty, notional_amount) VALUES(?, ?, ?, ?, ?)',
                        (deal.id, str(deal.deal_date), deal.deal_side, deal.qty, deal.notional_amount)
                    )
            # zapisz pozycje transakcji
            if deal.positions:
                for item in deal.positions:
                    try:
                        c.execute('INSERT INTO Deals_Positions (position, indeks_name, price, qty, amount, pricing_beg_date, pricing_end_date, deal_id) VALUES(?,?,?,?,?,?,?,?)',
                                        (item.position,item.indeks_name, item.price, item.qty, item.amount, item.pricing_beg_date, item.pricing_end_date, deal.id)
                                )
                    except Exception as e:
                        #print "item add error:", e
                        raise RepositoryException('error adding position item: %s, to deal: %s' %
                                                    (str(item), str(deal.id))
                                                )
        except Exception as e:
            #print "deal add error:", e
            raise RepositoryException('error adding deal %s' % str(deal))

    def delete(self, deal):
        """Metoda usuwa pojedynczą transakcję z bazy danych,
        wraz ze wszystkimi jej pozycjami.
        """
        try:
            c = self.conn.cursor()
            # usuń pozycje
            c.execute('DELETE FROM Deals_Positions WHERE position in (SELECT position FROM Deals_Positions WHERE deal_id=?)', (deal.id,))
            # usuń nagłowek
            c.execute('DELETE FROM Deals WHERE id=?', (deal.id,))
            return True
        except Exception as e:
            #print "deal delete error:", e
            raise RepositoryException('error deleting deal %s' % str(deal))
            return False
    #
    def getById(self, id):
        """Get transaction by id
        """
        try:
            c = self.conn.cursor()
            c.execute("SELECT * FROM Deals WHERE id=?", (id,))
            deal_row = c.fetchone()  #pobierz jeden wiersz
            deal = Deals(id=id)
            if deal_row == None:
                deal=None
            else:
                deal.deal_date = deal_row[1]
                deal.deal_side = deal_row[2]
                deal.qty=deal_row[3]
                deal.notional_amount=deal_row[4]
                c.execute("SELECT * FROM Deals_Positions WHERE deal_id=? order by position", (id,))
                deal_items_rows = c.fetchall()
                items_list = []
                for item_row in deal_items_rows:
                    item = Deals_Positions(position=item_row[0], indeks_name=item_row[1], price=item_row[2], qty=item_row[3],amount=item_row[4],pricing_beg_date=item_row[5],pricing_end_date=item_row[6])
                    items_list.append(item)
                deal.positions=items_list
        except Exception as e:
            #print "deal getById error:", e
            raise RepositoryException('error getting by id position_id: %s' % str(id))
        return deal

    def update(self, deal):
        """Metoda uaktualnia pojedynczą transakcję w bazie danych,
        wraz ze wszystkimi jej pozycjami.
        """
        try:
            # pobierz z bazy transakcję
            deal_oryg = self.getById(deal.id)
            if deal_oryg != None:
                # transakcja jest w bazie: usuń ją
                self.delete(deal)
            self.add(deal)

        except Exception as e:
            #print "deal update error:", e
            raise RepositoryException('error updating deal %s' % str(deal))

    #
    #
if __name__ == '__main__':
    try:
        with DealsRepository() as deal_repository:
#
            deal_repository.add(
                Deals(id = 100, deal_date = datetime.now(), deal_side="BUY",
                        positions = [
                            Deals_Positions(position="1", indeks_name = "GASOLINE10CIF",  price = 724.50, qty= 7680, amount = 5564160.0, pricing_beg_date = date(2015,5,1), pricing_end_date=date(2015,5,31)),
                            Deals_Positions(position="2", indeks_name = "GASOLINE10CIF",  price = 724.50, qty= 9580, amount = 6940710.0, pricing_beg_date = date(2015,6,1), pricing_end_date=date(2015,6,30)),
                            Deals_Positions(position="3", indeks_name = "GASOLINE10CIF",  price = 724.50, qty= 11500, amount = 8331750.0, pricing_beg_date = date(2015,7,1), pricing_end_date=date(2015,7,31)),
                        ]
                    )
                )

            deal_repository.add(
                Deals(id = 101, deal_date = datetime.now(), deal_side="BUY",
                        positions = [
                            Deals_Positions(position="4", indeks_name = "GASOLINE10CIF",  price = 724.50, qty= 7680, amount = 5564160.0, pricing_beg_date = date(2015,5,1), pricing_end_date=date(2015,5,31)),
                            Deals_Positions(position="5", indeks_name = "GASOLINE10CIF",  price = 724.50, qty= 9580, amount = 6940710.0, pricing_beg_date = date(2015,6,1), pricing_end_date=date(2015,6,30)),
                            Deals_Positions(position="6", indeks_name = "GASOLINE10CIF",  price = 724.50, qty= 11500, amount = 8331750.0, pricing_beg_date = date(2015,7,1), pricing_end_date=date(2015,7,31)),
                        ]
                    )
                )

            deal_repository.add(
                Deals(id = 102, deal_date = datetime.now(), deal_side="BUY",
                        positions = [
                            Deals_Positions(position="7", indeks_name = "BRENTDTD",  price = 48.74, qty= 10000, amount = 487400.0, pricing_beg_date = date(2015,5,1), pricing_end_date=date(2015,5,31)),
                            Deals_Positions(position="8", indeks_name = "BRENTDTD",  price = 48.74, qty= 15000, amount = 731100.0, pricing_beg_date = date(2015,6,1), pricing_end_date=date(2015,6,30)),
                            Deals_Positions(position="9", indeks_name = "BRENTDTD",  price = 50.0, qty= 20000, amount = 1000000.0, pricing_beg_date = date(2015,7,1), pricing_end_date=date(2015,7,31)),
                        ]
                    )
                )

            deal_repository.complete()
    except RepositoryException as e:
        print(e)

#select trade o numerze 100
print DealsRepository().getById(100)

if __name__ == '__main__':
   try:
       with DealsRepository() as deal_repository:
           deal_repository.update(
               Deals(id = 100, deal_date = datetime.now(), deal_side="SELL",
                       positions = [
                           Deals_Positions(position="1", indeks_name = "ULSD10CIF",  price = 626.50, qty= 4000, amount = 2506000.0, pricing_beg_date = date(2016,5,1), pricing_end_date=date(2016,5,31)),
                           Deals_Positions(position="2", indeks_name = "ULSD10CIF",  price = 626.50, qty= 3000, amount = 1879500.0, pricing_beg_date = date(2016,6,1), pricing_end_date=date(2016,6,30)),
                           Deals_Positions(position="3", indeks_name = "ULSD10CIF",  price = 626.50, qty= 1200, amount = 751800.0, pricing_beg_date = date(2016,8,1), pricing_end_date=date(2016,8,31)),

                       ]
                   )
               )
           deal_repository.complete()
   except RepositoryException as e:
       print(e)

if __name__ == '__main__':
   try:
       with DealsRepository() as deal_repository:
           deal_repository.delete(
               Deals(id = 102))
           deal_repository.complete()
   except RepositoryException as e:
       print(e)
