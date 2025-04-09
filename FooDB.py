from bs4 import BeautifulSoup as bs
import requests

CATALOG_PAGE = "https://foodb.ca/compounds?filter=true&quantified=1&page="
MET_PAGE = "https://foodb.ca/compounds/"

TOTAL_PAGES = 1

def parseWebpage():
    for page_num in range(0, TOTAL_PAGES):
        page = requests.get(CATALOG_PAGE + str(page_num))
        soup = bs(page.text, "html.parser")
        
            
        rows = soup.find_all("a", class_="btn-show")
        ids = [link.text for link in rows]

        print(f"DEBUG: Got ID's for page {page_num}")
        print(f"----------------------------------------------------------")
    
        for id in ids:
            print(f"DEBUG: Working with id: {id}")
            page = requests.get(MET_PAGE + id)
            soup = bs(page.text, features="xml")

            fooDB_name = getName(soup)
            met_class = getClass(soup)
            food_map = getAssociatedFoods(soup)

        #     print(f"""
        # Information for {id}:
        # fooDB Name: {fooDB_name}
        # Class: {met_class}
        # Associated Foods: {food_map}
        #         """)
        
    
def getName(soup):
    name = soup.find("name")
    if not name or not name.string:
        print("\tAlert: No Name")
        return
    return name.string

def getClass(soup):
    met_class = soup.find("class")
    if not met_class or not met_class.string:
        print("\tAlert: No Class")
        return
    return met_class.string

def getAssociatedFoods(soup):
    foods = {}
    food_table = soup.find("foods")
    for row in food_table.find_all("food"):
        name_tag = row.find("name")
        if not name_tag or not name_tag.string:
            print("\tAlert: No Food Name")
            continue
        name = name_tag.string
        foods[name] = {}
        average_value = row.find("average_value")
        if not average_value or not average_value.string:
            print("\tAlert: No Average value")
        else:
            foods[name]["average_value"] = average_value.string
            
        max_value = row.find("max_value")
        if not max_value or not max_value.string:
            print("\tAlert: No Max value")
        else:
            foods[name]["max_value"] = max_value.string
            
        min_value = row.find("min_value")
        if not min_value or not min_value.string:
            print("\tAlert: No Min value")
        else:
            foods[name]["min_value"] = min_value.string
        if foods[name]["min_value"] == '0.0' and foods[name]["max_value"] == '0.0':
            del foods[name]
        
    return foods

if __name__ == "__main__":
    parseWebpage()