import xml.etree.ElementTree as ET

import requests


# Create element tree object
root = ET.fromstring(
    requests.get("https://cfconventions.org/Data/cf-standard-names/84/src/cf-standard-name-table.xml").content
)

# Find all 'entry' elements and get their 'id' attribute
with open("cf_standard_names_v84.txt", "w") as file:
    for entry in root.findall("entry"):
        file.write(f'{entry.get("id")}\n')
