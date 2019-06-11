# Inflammatory bowel disease (IBD)

## Ontologies
- To get the ontologies first get pytag from https://github.com/KociOrges/pytag
- Follow the installation guide
- It is necessary to pay attention on the bibtexparser library, its necessary to get the exact version 0.6.2.
- Go to Pubmed https://www.ncbi.nlm.nih.gov/pubmed/
- Find the Advanced Search and enter the disease on the first textbox, on the second enter for example "Food".
- At the top there is a Send To link, find Citation Manager and export the results.
- Enter on EndNote and import the File, add the Accession Number and export to bibtex.
- Go to the terminal and enter:

```
 pytag --input_dir ~/yourbibtexfolder --onto_types all --out_file outputfile.tsv

```
- For more detailed information on using pytag go to https://github.com/KociOrges/pytag

- To run the statistics the result file needs two more columns: The first describing the Pubmed filter (Diet, Food, Intolerance, Nutrition and Probiotics) and the second with the disease.

- Save it to csv and import on WEKA. Run the cluster and get the statistics results.

### Paper:
  https://www.ncbi.nlm.nih.gov/pmc/articles/PMC6064635/
  
### Local: 

## Complex Network

### Dataset:
  Nhanes 2007-2008: https://wwwn.cdc.gov/nchs/nhanes/continuousnhanes/default.aspx?BeginYear=2007

  Nhanes 2009-2010: https://wwwn.cdc.gov/nchs/nhanes/continuousnhanes/default.aspx?BeginYear=2009

### Local:
  CSV generation:
  python -m xport <filename>.XPT > <filename>.csv

  Database generation:
  python create_database.py

  Queries:
  python correlation.py

### File: 
  [network.ipynb](https://github.com/Trindad/digestive-diseases/blob/master/network/network.ipynb)
  
### Dependency:
  To run locally, you need to install all dependencies listed in [requirements.txt](https://github.com/Trindad/digestive-diseases/blob/network/requirements.txt)

### Jupyter:
[![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/Trindad/digestive-diseases/network)


## Machine Learning

### Dataset:
  Paper: https://www.nature.com/articles/s41598-017-02606-2.pdf

## Google colab: 

## Statistics

 The [statistics](https://github.com/Trindad/digestive-diseases/blob/network/network/statistics_and_correlation.ipynb) are inside the jupyter notebook used for the complex network.
