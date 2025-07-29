from typing import Dict

food_memo = {}
class_memo: Dict[str, int] = {None: None}
biospec_memo: Dict[str, int] = {}
foodcat_memo: Dict[str, int] = {}

HMDB_CATALOG_PAGE = "https://hmdb.ca/metabolites?blood=1&c=hmdb_id&d=up&filter=true&food=1&quantified=1&page="
HMDB_MET_PAGE = "https://hmdb.ca/metabolites/"
HMDB_START_PAGE = 1
HMDB_TOTAL_PAGES = 87

FOODB_CATALOG_PAGE = "https://foodb.ca/compounds?filter=true&quantified=1&page="
FOODB_MET_PAGE = "https://foodb.ca/compounds/"
FOODB_START_PAGE = 1
FOODB_TOTAL_PAGES = 151

FOODDB_FOOD_CATALOG_URL = "https://foodb.ca/foods?button=&c=food_group&d=up&page="
FOODB_FOOD_TOTAL_PAGES = 32
