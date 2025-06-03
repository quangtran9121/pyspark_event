import pycountry
import csv

# Define the CSV file name
csv_file = 'country_codes.csv'

# Define the header
header = ['Name', 'Alpha-2', 'Alpha-3', 'Numeric']

# Open the CSV file for writing
with open(csv_file, mode='w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)
    writer.writerow(header)  # Write the header
    
    # Iterate through all countries and write their codes
    for country in pycountry.countries:
        writer.writerow([country.name, country.alpha_2, country.alpha_3, country.numeric])

print(f"All country codes have been saved to {csv_file}")
