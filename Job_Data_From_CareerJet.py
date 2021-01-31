# importing modules and libraries for further work with them
import requests
from bs4 import BeautifulSoup
import pandas as pd

# varibale and list declaration
job_list = []
data_all = []

print("Input job title, keywork or company name : ")
job = str(input())
print("Input city or country to see the result : ")
country = str(input())
# Url for target scrapping 
search = "https://www.careerjet.com.bd/search/jobs?s="+job+"&l="+country
print(search)

# Http request for getting the raw data from the website 
r = requests.get(search)
r.status_code

# parsing the html document using the html parser 
soup = BeautifulSoup(r.text, 'html.parser')

# lets findout each job post container in a list
a = soup.find_all("article", class_=["job","clicky"])

# spliting each item and converting into a list for further processing
for i in a:
    job_list.append([i])

# iterating over all the list items and extracting the value from those
for each_list_item in job_list:
    for k in each_list_item:
        job_title = k.find('a').string.strip()
        
        # checking whether the company is empty or not
        try:
            company = k.find('p', class_="company").string.strip()
        except:
            company = " "
        
        # checking whether the country is empty or not
        details = k.find('ul', class_="details")
        try:
            country = details.find_all("li")[0].get_text().strip()
        except:
            country = " "
            
        # checking salary the country is empty or not
        try:
            salary = details.find_all("li")[1].get_text().strip()
        except:
            salary = " "
            
        # checking information regarding description
        description = k.find("div", class_="desc").get_text().strip()
        
        #appending all the values in form of list to the data_all list to create DataFrame in future
        data_all.append([job_title, company, salary, country, description])
        

# lets store all the data in the pandas dataframe
df = pd.DataFrame(data_all, columns=["Job_Title", "Company", "Salary", "Location", "Description | Job Responsibilities"])
df.to_csv("Job_Data_From_CareerJet.csv")