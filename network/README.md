# Inflammatory bowel disease

Nhanes 2007-2008: https://wwwn.cdc.gov/nchs/nhanes/continuousnhanes/default.aspx?BeginYear=2007

Nhanes 2009-2010: https://wwwn.cdc.gov/nchs/nhanes/continuousnhanes/default.aspx?BeginYear=2009

CSV generation:
python -m xport <filename>.XPT > <filename>.csv

Database generation:
python create_database.py

Queries:
python correlation.py
