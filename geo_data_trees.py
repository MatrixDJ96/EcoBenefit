import json
import csv

# Definisco il path del file
input_path = 'geo_data_trees.geojson'
output_path = 'geo_data_trees.csv'

# Apro uno stream sul file da leggere
input_file = open(input_path, 'r')
# Apro uno stream sul file da scrivere
output_file = open(output_path, 'w')

# Leggo il file come JSON
j = json.load(input_file)

# Creo il file CSV
csv = csv.writer(output_file, delimiter=';')

# Indica se devo scrivere l'intestazione del CSV
write_header = True

# Scrive le righe nel CSV
for item in j['features']:
    if write_header:
        write_header = False
        csv.writerow(item['properties'])
    csv.writerow(item['properties'].values())

# Chiudo lo stream del file
input_file.close()
