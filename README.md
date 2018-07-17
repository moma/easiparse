# Easiparse

## What for ?

Parsing and importing to mongodb any flat text file following a flexible configuration. A flat text file is for example MEDLINE format from pubmed.gov : 

  PMID- 19712554
  OWN - NLM
  STAT- MEDLINE
  DA  - 20090828
  DCOM- 20091109
  IS  - 1472-6491 (Electronic)
  ...

## Dependencies

 * Python 2.6
 * MongoDB, and pymongo

## How-to

 * configure easiparse editing config.yaml
 * launch mongodb server
 * launch the software : 
 $ python easiparse.py
