from bs4 import BeautifulSoup as bs
import requests
import re

CATALOG_PAGE = "https://hmdb.ca/metabolites?blood=1&c=hmdb_id&d=up&filter=true&food=1&quantified=1&page="
MET_PAGE = "https://hmdb.ca/metabolites/"
MET_FOODB_PAGE = "https://foodb.ca/compounds/"
TOTAL_PAGES = 1

def parseWebpage():
    for page_num in range(1, TOTAL_PAGES+1):
        page = requests.get(CATALOG_PAGE + str(page_num))
        soup = bs(page.text, "html.parser")
        met_link = soup.find_all("td", class_="metabolite-link")
        ids = [link.a.text for link in met_link]
        print(f"DEBUG: Got ID's for page {page_num}")
        print(f"----------------------------------------------------------")

    for id in ids:
        print(f"DEBUG: Working with id: {id}")
        page = requests.get(MET_PAGE + id + ".xml")
        soup = bs(page.text, features="xml")

        common_name = getName(soup)
        loci = getLoci(soup)
        food_map = getFoodSources(soup)
        concentrations = getConcentrations(soup, True)
        abconcentrations = getConcentrations(soup, False)
        
        print(f"""
    Information for {id}:\n
    Common Name: {common_name}\n
    Biospecimen Location: {loci}\n
    Food Source: {food_map}\n
    Concentrations: {concentrations}\n
    AbConentrations: {abconcentrations}\n
            """)
        
    
def getName(soup):
    name = soup.find("name")
    if not name:
        print("\tAlert: No Name")
    return name.text

def getLoci(soup):
    location_tags = soup.find("biospecimen_locations")
    if not location_tags:
        print("\tAlert: No Biospecimen locations")
    locations = [loc.text for loc in location_tags.find_all("biospecimen")]
    return locations

def getFoodSources(soup):
    foodb_id = soup.find("foodb_id")
    if not foodb_id or foodb_id.string:
        print("\tAlert: No Food ID")
        return
    page = requests.get(MET_FOODB_PAGE + foodb_id.string)
    
    foodb_soup = bs(page.text, "xml")
    food_tags = foodb_soup.find("foods")
    foods = []
    if not food_tags:
        print("\tError: No food data")
        
    for food in food_tags.find_all("food"):
        foods.append(food.find("name").string)
    return foods

def getConcentrations(soup, normal = True):
    # Details header ignored
    headers = ["biospecimen", "Status", "Value", "Age", "Sex", "Condition", "Reference"]

    def meetsCriteria(tag):
        """Currently looks for only in blood and that it can be quantified

        Args:
            tag (bs4.element.tag): the tag being filtered
        """
        return tag.name == "td" and tag.text == "Detected and Quantified"
    conc_table = soup.find("normal_concentrations") if normal else soup.find("abnormal_concentrations")
    
    if not conc_table:
        print(f"\tAlert: {'Normal Concentrations' if normal else 'Abnormal Concentrations'} missing")
    concs = conc_table.find_all("concentration")
    if not concs:
        print(f"\tError: Concentrations missing")
        
    concentrations = []
    
    for conc in concs:
        concentration = {}
        for col in conc.find_all(True):
            if col.name == "references":
                concentration["references"] = []
                references = col.find_all("reference")
                if len(references) != 1:
                    print(f"\tAlert: More than one reference")
                for reference in references:
                    ref = {}
                    for ref_col in reference.find_all(True):
                        ref[ref_col.name] = ref_col.text
                    concentration["references"].append(ref)
            else:
                concentration[col.name] = col.text
        concentrations.append(concentration)
    return concentrations

if __name__ == "__main__":
    parseWebpage()
