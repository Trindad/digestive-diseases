# Inflammatory bowel disease: complex network


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
  
### Dependency:
  To run locally, you need to install all dependencies listed in [requirements.txt](https://github.com/Trindad/digestive-diseases/blob/network/requirements.txt)

### Jupyter:
[![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/Trindad/digestive-diseases/network)
