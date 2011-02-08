#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os, sys
import re

import logging
logging.basicConfig(level=logging.DEBUG, format="%(levelname)-8s %(message)s")
import os, sys

from pymongo import Connection
MONGODB_PORT = 27017

def create_bdd(name_database):
   return Connection("localhost", MONGODB_PORT)[name_database]

def convert_ligne(string):
   newstring= string.replace('$ligne$','\n')
   return newstring

def lire_parametre_ini(fichier_name_ini):
   file_in = open(fichier_name_ini,'r')
   lignes = file_in.readlines()
   dico_tag={}
   for ligne in lignes:
      line = ligne[:-1]
      line_v=line.split('\t')

      if len(line_v)>1:
         if len(line_v)>4:
            dico_tag[line_v[0]]=(line_v[1],list(map(convert_ligne,line_v[2:])))
            #dico_tag[line_v[0]]=(line_v[1],map(convert_ligne,line_v[2:]))
         dico_tag[line_v[0]]=(line_v[1],list(map(convert_ligne,line_v[2:])))
   return dico_tag

def nettoyer(item):
   element_propre = (item.strip().replace("\n",' '))
   while '  ' in element_propre:
      element_propre=element_propre.replace('  ',' ')
   return element_propre

def clean(contenu_tag):
   if type(contenu_tag) != list:# si contenu_tag est une simple chaîne de caractère on crée une liste d'un élément contenant cette chaîne de caractère
      contenu_tag_clean = nettoyer(contenu_tag)
   else:
      contenu_tag_clean=[]
      for x in contenu_tag:
         if type(x) != list:
            contenu_tag_clean.append(nettoyer(x))
         else:
            contenu_tag_clean_sub=[]
            for y in x:
               contenu_tag_clean_sub.append(nettoyer(y))
            contenu_tag_clean.append(contenu_tag_clean_sub)
   return contenu_tag_clean

def get_info_split(chaine,sep):
   if sep!='':
      infosep=chaine.split(sep)#on split par rapport au séparateur courant
   else:
      infosep=[chaine]
   if '' in infosep:#on enleve les champs vides
      infosep.remove('')
   return infosep


def parser_contenu_tag(contenu_tag, dico_tag, tag_isi_current, longueur_tag, i=0):
   """
   on retire le dit tag du contenu si on n'a pas déjà effectué une étape de découpage sur notre chaine, cas C1

   """
   contenu_tag = contenu_tag[longueur_tag:]
   #ce sont les séparateurs déclarés dans le fichier de paramètre
   separateurs = dico_tag[tag_isi_current][1]
   # le nombre de séparateurs, c'est le nombre de valeurs non nulles sur les deux premiers éléments de la liste separateurs
   NN = len(separateurs[:2])-separateurs[:2].count('')
   # on itère sur les deux premiers séparateurs
   for sep in separateurs[:NN]:
      if '***' in sep:
         #au cas où il y aurait différent séparateur au même niveau ex: C1: , et ;
         sep = sep.split('***')[i]
      infos = []
      # si contenu_tag est une simple chaîne de caractère on crée une liste d'un élément contenant cette chaîne de caractère
      if type(contenu_tag) != list:
         infosep = get_info_split(contenu_tag,sep)
         infos = infosep
      else:
         #on itère sur chaque élément de contenu_tag, contenu_tag[:] fait une copie de la liste
         for sub_contenu_tag in contenu_tag[:]:
            infosep = get_info_split(sub_contenu_tag,sep)
            infos.append(infosep)
      contenu_tag = infos
   contenu_tag_clean = clean(contenu_tag)

   return normalisell(contenu_tag_clean)

def normalisell(variable):
   if len(variable)==0:
      variable = [['']]
   else:
      if type(variable) != list:
         variable = [variable]
      if type(variable[0]) != list:
         variable = [variable]
   return variable

def find_between( s, first, last ):
    try:
        start = s.index( first ) + len( first )
        end = s.index( last, start )
        return s[start:end],end+1
    except ValueError:
        return "",0


def divide(chaine,marqueur_cut,sep):
   marque = marqueur_cut.split('***')
   left,right = [],[]
   for ligne in chaine.split(sep):
      betw,cut = find_between(ligne,marque[0],marque[1])
      left.append(betw)
      right.append(ligne[cut:])
   return sep.join(left),sep.join(right)

def main(file_isi, bdd_name, dico_tag, match_regexp, limit=None, overwrite=False):

   match_regexp = re.compile(match_regexp, re.I|re.U)

   match_fields = ["TI","AB"]

   mongodb = create_bdd(bdd_name)
   if overwrite is True and "notices" in mongodb.collection_names():
      mongodb.drop_collection("notices")

   longueur_tag = int(dico_tag['longueur_tag'][0])
   tag_isi = [] # initialise la variable tag_isi qui comprend tous les tags qui nous intéressent.
   for x in dico_tag.keys():
      if len(x) == longueur_tag:
         tag_isi.append(x)

   tag_end = convert_ligne(dico_tag['tag_end'][0])
   tag_begin = convert_ligne(dico_tag['tag_begin'][0])
   contenu_tag = '' # initialise la variable contenu_tag
   tag_isi_current = '' # initialise la variable tag_isi_current
   intag = 0 # initialise la variable intag
   total_imported = 0 # initialise la variable total_imported

   file_isi_lignes = file_isi.readlines()
   notice = {}
   for ligne in file_isi_lignes:# itere sur les lignes du corpus
      debut_tag = dico_tag['condition_debut_tag'][0]  # debut tag = condition expression régulière
      nfirst = ligne[:longueur_tag] # nprems = n première lettres de la ligne, n=longueur_tag
      if re.match(debut_tag, ligne) != None and nfirst != tag_isi_current: # vérifie que le début de la ligne correspond bien à un tag qqu'il soit et que ce tag n'est pas une répétition (données type scopus ou medline)
         intag=0 # fin d'un tag
         if tag_isi_current in tag_isi: # condition pour éviter de renvoyer un contenu vide qui correspondrait à un tag précédent ne faisant pas partie de la sélection de parameters.ini
            if len(dico_tag[tag_isi_current][1]) > 2:#cas spécial, il faut spliter la chaine avant toute chose
               contenu_tag_left, contenu_tag_right = divide(contenu_tag,dico_tag[tag_isi_current][1][2],dico_tag[tag_isi_current][1][0])
               contenu_tag_left += tag_isi_current
               contenu_tag_right += tag_isi_current
               for j,contenu_tag in enumerate([contenu_tag_left,contenu_tag_right]):
                  notice[tag_isi_current +'uuu'+str(j)] = parser_contenu_tag(contenu_tag, dico_tag, tag_isi_current, longueur_tag)
            else:
               notice[tag_isi_current] = parser_contenu_tag(contenu_tag, dico_tag, tag_isi_current, longueur_tag)
            tag_isi_current = '?'
      if nfirst in tag_isi and nfirst != tag_isi_current: #on vérifie que le tag du début de la ligne fait bien partie des tags pertinents et que le tag en question n'est pas une répétition (données type scopus ou medline)
         intag = 1 # on ouvre un nouveau tag
         contenu_tag = '' # on initialise son contenu
         tag_isi_current = nfirst # on déclaire le tag courant
      if intag == 1:
         contenu_tag += ligne # si la variable intag n'a pas été annulée parce qu'un nouveau tag aurait été rencontré, on concatène la ligne dans contenu_tag
      if nfirst == tag_begin:
         if limit is not None:
            if total_imported>limit:
               break
         if not total_imported%100:
            print ( "%d notices indexés"%total_imported)
         notice = {} #on initialise la variable notice qui regroupe dans un dictionnaire les valeurs de la notice courante pour toutes les clés déclarées
      if nfirst == tag_end:
         total_imported += filter_notice(match_fields, match_regexp, notice, mongodb)
   return total_imported

def filter_notice(match_fields, match_regexp, notice, mongodb):
   """
   search for an expression into the required fields of notice
   """
   for tag in match_fields:
      if tag not in notice:
         logging.debug("notice incomplete")
         return 0
   for tag in match_fields:
      if match_regexp.match(notice[tag]):
         logging.debug("matched %s in notice %s"%(str(match_regexp),notice['TI']))
         mongodb.notices.save(notice)
         return 1
