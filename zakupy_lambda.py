# -*- coding: utf-8 -*-
oferta={'chleb':2.5,'mleko':1.99,'wiejski':4.15, 'platki':10}
portfel=100
f=(lambda x,y: x*y)
g=(lambda x,y: x+y)
def zakupy(oferta,portfel,**listaZakupow):
  kupno={k:0 for k in listaZakupow}
  nazwy=[k for k in listaZakupow] 
  ilosc,cena=[listaZakupow[k] for k in listaZakupow],[oferta[k] for k in listaZakupow]   
  wartosc=map(f,ilosc,cena)    
  suma=reduce(g,wartosc,0)    
  return [(nazwy[i],wartosc[i]) for i in range(len(nazwy))], (portfel-suma)
#    lista_kupno=[(lambda x,y:x*y)(listaZakupow[k],oferta[k]) for k in listaZakupow]
print zakupy(oferta,portfel,chleb=2, mleko=3, wiejski=1)
