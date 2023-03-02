# Databricks notebook source
# MAGIC %pip install lxml

# COMMAND ----------

import requests
import json
from bs4 import BeautifulSoup
import lxml
import pandas as pd

# COMMAND ----------

# MAGIC %md 
# MAGIC Step 1: Get URLs from Website

# COMMAND ----------

response = requests.get("https://www.kingarthurbaking.com/sitemap.xml")

# COMMAND ----------

response.text
master_sitemap_soup = BeautifulSoup(response.text, 'lxml')
sitemap_pages_list = [page.text for page in master_sitemap_soup.find_all("loc")]

url_location_list = []
last_modified_list = []
change_freq_list = []
priority_list = []

for page in sitemap_pages_list:
  page_response = requests.get(page)
  sitemap_soup = BeautifulSoup(page_response.text, 'lxml')

  for row in sitemap_soup.find_all('url'):
    url_location = row.loc.text if row.loc is not None else None
    last_modified = row.lastmod.text if row.lastmod is not None else None
    change_freq = row.changefreq.text if row.changefreq is not None else None
    priority = row.priority.text if row.priority is not None else None
    
    url_location_list.append(url_location)
    last_modified_list.append(last_modified)
    change_freq_list.append(change_freq)
    priority_list.append(priority)


df = pd.DataFrame({'url_location' : url_location_list, 'last_modified' : last_modified_list, 'change_freq' : change_freq_list, 'priority' : priority_list})
display(df)

# COMMAND ----------

display(df[df['url_location'].str.contains("www.kingarthurbaking.com/recipes/")])

# COMMAND ----------

# MAGIC %md
# MAGIC Step 2: Persist raw HTML to S3

# COMMAND ----------

recipe_name = "five-grain-bread-with-pate-fermentee-recipe"
response = requests.get(f"https://www.kingarthurbaking.com/recipes/{recipe_name}")
response_html = response.text

raw_html_path_root = "s3://oetrta/justinkolpak/bread_stuff/raw/king_arthur/html/"
raw_html_path_full = raw_html_path_root + recipe_name + ".html"

dbutils.fs.put(raw_html_path_full, response_html, True)

# COMMAND ----------

# MAGIC %md
# MAGIC Step 3: Extract JSON from HTML and persist to S3

# COMMAND ----------

temp_location_path_root = "/dbfs/FileStore/justin.kolpak/"
temp_location_path_full = temp_location_path_root + recipe_name + ".html"

dbutils.fs.cp(raw_html_file_path_full, f"file:{temp_location_path_full}")

f = open(temp_location_path_full, "r")
soup = BeautifulSoup(f.read(), 'html.parser')

raw_json = json.dumps(json.loads(soup.find('script', type='application/ld+json').text))

raw_json_path_root = "s3://oetrta/justinkolpak/bread_stuff/raw/king_arthur/json/"
raw_json_path_full = raw_json_path_root + recipe_name + ".json"

dbutils.fs.put(raw_json_path_full, raw_json, True)

# COMMAND ----------

# Clean up Temp File if all above processing is successful
dbutils.fs.rm(f"file:{temp_location_path_full}")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC   , _metadata.file_path
# MAGIC   , _metadata.file_name
# MAGIC   , _metadata.file_size
# MAGIC   , _metadata.file_modification_time
# MAGIC FROM json.`s3://oetrta/justinkolpak/bread_stuff/raw/king_arthur/json/five-grain-bread-with-pate-fermentee-recipe.json`
