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

    for id in ids:
        page = requests.get(MET_PAGE + id)
        soup = bs(page.text, features="xml")

        fooDB_name = getName(soup)
        met_class = getClass(soup)
        food_map = getAssociatedFoods(soup)

        print(f"""
    Information for {id}:
    fooDB Name: {fooDB_name}
    Class: {met_class}
    Associated Foods: {food_map}
            """)
        
    
def getName(soup):
    return soup.find("name").text

def getClass(soup):
    return soup.find("class").text

def getAssociatedFoods(soup):
    foods = {}
    food_table = soup.find_all("foods")
    for row in food_table:
        name = row.find("name").text
        foods[name] = {}
        foods[name]["average_value"] = row.find("average_value").text
        foods[name]["max_value"] = row.find("max_value").text
        foods[name]["min_value"] = row.find("min_value").text
    return foods

if __name__ == "__main__":
    parseWebpage()