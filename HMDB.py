from bs4 import BeautifulSoup as bs
import requests
import re

CATALOG_PAGE = "https://hmdb.ca/metabolites?blood=1&c=hmdb_id&d=up&filter=true&food=1&quantified=1&page="
MET_PAGE = "https://hmdb.ca/metabolites/"

TOTAL_PAGES = 1

def parseWebpage():
    for page_num in range(0, TOTAL_PAGES):
        page = requests.get(CATALOG_PAGE + str(page_num))
        soup = bs(page.text, "html.parser")
        met_link = soup.find_all("td", class_="metabolite-link")
        ids = [link.a.text for link in met_link]
        print(f"DEBUG: Got ID's for page {page_num}")
        print(f"----------------------------------------------------------")

    for id in ids:
        print(f"DEBUG: Working with id {id}")
        page = requests.get(MET_PAGE + id)
        soup = bs(page.text, "html.parser")

        common_name = getName(soup)
        loci = getLoci(soup)
        food_map = getFoodSources(soup)
        concentrations = getConcentrations(soup, "Normal Concentrations")
        abconcentrations = getConcentrations(soup, "Abnormal Concentrations")
            
        print(f"""
    Information for {id}:\n
    Common Name: {common_name}\n
    Biospecimen Location: {loci}\n
    Food Source: {food_map}\n
    Concentrations: {concentrations}\n
    AbConentrations: {abconcentrations}\n
            """)
        
    
def getName(soup):
    common_name_tag = soup.find(string="Common Name").parent
    return common_name_tag.parent.td.text

def getLoci(soup):
    loci_tag = soup.find(string="Biospecimen Locations").parent
    return [locus.text for locus in loci_tag.parent.find_all("li")]

def getFoodSources(soup):
    disposition_tag = soup.find(string="Disposition").parent
    food_tag = disposition_tag.parent.next_sibling.find_all(string="Food")[1].parent
    if food_tag:
        food_table = food_tag.parent.parent.find("ul", class_="category-ontnode")
        food_category_tags = food_table.find_all("a", class_="category-ontnode")
        food_map = {}
        for cat in food_category_tags:
            cat_name = cat.string
            food_map[cat_name] = []
            
            for tag in cat.next_sibling.find_all(True):
                if(tag.get("class") and tag.get("class")[0] == "leaf-ontnode"):
                    food_map[cat_name].append(tag.string)
    else:
        print("No Food source")

def getConcentrations(soup, name="Normal Concentrations"):
    # Details header ignored
    headers = ["Biospecimen", "Status", "Value", "Age", "Sex", "Condition", "Reference"]

    def meetsCriteria(tag):
        """Currently looks for only in blood and that it can be quantified

        Args:
            tag (bs4.element.tag): the tag being filtered
        """
        return tag.name == "td" and tag.text == "Detected and Quantified"
    
    normal_conc_tag = soup.find(string=name).parent
    conc_table = normal_conc_tag.parent.next_sibling.find("table")
    conc_rows = conc_table.find_all(meetsCriteria)
    
    concentrations = []
    for tag in conc_rows:
        columns = tag.parent.find_all(True)
        concentration_info = {}
        for header, col in zip(headers, columns):
            href_tag = col.find("a", {"href": True})
            title_tag = col.find("span", {"title": True})
            if href_tag:
                concentration_info[header] = href_tag.get("href")
            elif title_tag:
                concentration_info[header] = title_tag.get("title")
            else:
                concentration_info[header] = col.text
        concentrations.append(concentration_info)
        
    return concentrations

if __name__ == "__main__":
    # parseWebpage
    page = requests.get("https://hmdb.ca/metabolites/HMDB0000010.xml")
    soup = bs(page.text, "html.parser")
    with open("test.html", "w") as file:
        file.write(soup.prettify()) 
    
