import array
import csv
import json
from pathlib import Path


def read_data_from_json(path: str):
    # Apro uno stream sul file da leggere
    stream = open(path, 'r')
    # Leggo il file come JSON
    data = json.load(stream)
    # Chiudo lo stream del file
    stream.close()
    return data


def increment_item_value(data: dict, name: str, value: float = 1):
    if data.get(name):
        value += data[name]
    data[name] = value


class CsvHelper:
    def __init__(self):
        self.rows = []
        self.header = []

    def set_header(self, header: array):
        self.header = header

    def set_rows(self, row: array):
        self.rows = row

    def add_row(self, row: array):
        self.rows.append(row)

    def save_file(self, name: str, delimiter: str = ',', lineterminator: str = "\n"):
        # Definisco il path del file
        path = Path(name).with_suffix('.csv')
        # Apro uno stream sul file da scrivere
        stream = open(path, 'w')
        # Creo il writer CSV
        writer = csv.writer(stream, delimiter=delimiter, lineterminator=lineterminator)
        # Scrivo l'intestazione
        if self.header:
            writer.writerow(self.header)
        # Scrivo le righe
        for row in self.rows:
            writer.writerow(row)
        # Chiudo lo stream del file
        stream.close()


class GeoDataHelper:
    # Nomi dei file JSON
    tree_path: Path = None

    # Dati estratti dal JSON
    tree_data: dict = {}

    # Dati elaborati dal JSON
    tree_categories: dict = {}

    def set_tree_path(self, path: str):
        self.tree_path = Path(path)

    def set_tree_data(self, force: bool = False):
        if force or not self.tree_data:
            self.tree_data = read_data_from_json(self.tree_path.name)

    def set_tree_categories(self, force: bool = False):
        if force or not self.tree_categories:
            self.set_tree_data()
            self.tree_categories = {}
            for item in self.tree_data['features']:
                name = item['properties']['Name']
                canopy = item['properties']['Canopy Cover (m2)']
                if name.lower() != 'total':
                    if not self.tree_categories.get(name):
                        self.tree_categories[name] = {}
                    increment_item_value(self.tree_categories[name], 'Abundance', 1)
                    increment_item_value(self.tree_categories[name], 'Canopy', float(canopy))
            for tree, data in self.tree_categories.items():
                data['Canopy'] = data['Canopy'] / data['Abundance']

    def create_full_csv(self, path: str):
        self.set_tree_data()
        # Creo il l'oggetto CsvHelper
        csv_helper = CsvHelper()
        # Indica se devo scrivere l'intestazione del CSV
        write_header = True
        # Scrive le righe nel CSV
        for item in self.tree_data['features']:
            if write_header:
                write_header = False
                csv_helper.set_header(item['properties'])
            csv_helper.add_row(item['properties'].values())
        # Salvo il file csv
        csv_helper.save_file(path)

    def create_tree_categories_csv(self, path: str):
        self.set_tree_categories()
        # Creo il l'oggetto CsvHelper
        csv_helper = CsvHelper()
        # Scrive i dati nel CSV
        csv_helper.set_header(['Name', 'Abundance', 'Canopy'])
        for tree, data in self.tree_categories.items():
            csv_helper.add_row([tree] + list(data.values()))
        # Salvo il file csv
        csv_helper.save_file(path)


if __name__ == '__main__':
    # Definisco il path del file
    tree_path = Path('geo_data_trees.geojson')

    # Istanzio l'oggetto GeoDataHelper
    geo_data_helper = GeoDataHelper()

    # Imposto il path del file
    geo_data_helper.set_tree_path(tree_path.name)

    # Creo CSV con la lista dei nomi e il numero di alberi
    geo_data_helper.create_tree_categories_csv('csv/' + tree_path.stem + '_categories')

