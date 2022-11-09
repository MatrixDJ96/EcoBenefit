import array
import csv
import json
from pathlib import Path
from shapely.geometry import shape, Point


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
    neighborhood_path: Path = None

    # Dati estratti dal JSON
    tree_data: dict = {}
    neighborhood_data: dict = {}

    # Dati elaborati dal JSON
    tree_categories: dict = {}
    tree_neighborhoods: dict = {}

    top_trees: list = []
    other_tree_name: str = 'Others'

    def set_tree_path(self, path: str):
        self.tree_path = Path(path)

    def set_tree_data(self, force: bool = False):
        if force or not self.tree_data:
            self.tree_data = read_data_from_json(self.tree_path.name)

    def set_neighborhood_path(self, path: str):
        self.neighborhood_path = Path(path)

    def set_neighborhood_data(self, force: bool = False):
        if force or not self.neighborhood_data:
            self.neighborhood_data = read_data_from_json(self.neighborhood_path.name)

    def set_top_trees_limit(self, limit: int):
        self.set_tree_categories()
        # Ordino gli alberi per numero
        values = sorted(self.tree_categories.items(), key=lambda x: x[1]['Abundance'], reverse=True)
        # Estraggo il nome degli alberi più diffusi
        self.top_trees = list(dict(values[:limit]).keys())

    def set_tree_categories(self, force: bool = False):
        if force or not self.tree_categories:
            self.set_tree_data()
            self.tree_categories = {}
            for item in self.tree_data['features']:
                name = item['properties']['Name']
                canopy = item['properties']['Canopy Cover (m2)']
                carbon = item['properties']['Carbon Storage (kg)']
                height = item['properties']['Height (m)']
                if name.lower() != 'total':
                    if not self.tree_categories.get(name):
                        self.tree_categories[name] = {}
                    increment_item_value(self.tree_categories[name], 'Abundance', 1)
                    increment_item_value(self.tree_categories[name], 'Canopy', float(canopy))
                    increment_item_value(self.tree_categories[name], 'Carbon', float(carbon))
                    increment_item_value(self.tree_categories[name], 'Height', float(height))
            for tree, data in self.tree_categories.items():
                data['Canopy'] = data['Canopy'] / data['Abundance']
                data['Carbon'] = data['Carbon'] / data['Abundance']
                data['Height'] = data['Height'] / data['Abundance']

    def set_trees_neighborhoods(self, force: bool = False):
        if force or not self.tree_neighborhoods:
            self.set_tree_data()
            self.set_neighborhood_data()
            self.tree_neighborhoods = {}
            # Creo la lista delle circoscrizioni
            polygons = {}
            for item in self.neighborhood_data['features']:
                if item['geometry'] and item['geometry']['coordinates']:
                    polygons[item['properties']['nome']] = shape(item['geometry'])
            # Elaboro in numero di alberi per circoscrizione
            for item in self.tree_data['features']:
                if item['geometry'] and item['geometry']['coordinates']:
                    point = shape(item['geometry'])
                    name = item['properties']['Name']
                    for circoscrizione, polygon in polygons.items():
                        # Inizializzo la circoscrizione
                        if not self.tree_neighborhoods.get(circoscrizione):
                            self.tree_neighborhoods[circoscrizione] = {}
                            for tree in self.top_trees:
                                self.tree_neighborhoods[circoscrizione][tree] = 0
                            self.tree_neighborhoods[circoscrizione][self.other_tree_name] = 0
                        # Verifico l'albero fa parte della circoscrizione
                        if polygon.contains(point):
                            if name not in self.top_trees:
                                name = self.other_tree_name
                            increment_item_value(self.tree_neighborhoods[circoscrizione], name)
                            break

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
        csv_helper.set_header(['Name', 'Abundance', 'Canopy', 'Carbon', 'Height'])
        for tree, data in self.tree_categories.items():
            csv_helper.add_row([tree] + list(data.values()))
        # Salvo il file csv
        csv_helper.save_file(path)

    def create_tree_neighborhoods_csv(self, limit: int, path: str):
        self.set_top_trees_limit(limit)
        self.set_trees_neighborhoods()
        # Creo il l'oggetto CsvHelper
        csv_helper = CsvHelper()
        # Scrive la testata nel CSV
        header = ['Neighborhood'] + self.top_trees.copy() + [self.other_tree_name]
        csv_helper.set_header(header)
        # Scrive le righe nel CSV
        for circoscrizione, trees in self.tree_neighborhoods.items():
            csv_helper.add_row([circoscrizione] + list(trees.values()))
        # Salvo il file csv
        csv_helper.save_file(path)


if __name__ == '__main__':
    # Definisco i path del file
    tree_path = Path('geo_data_trees.geojson')
    neighborhood_path = Path('circoscrizioni.json')

    # Istanzio l'oggetto GeoDataHelper
    geo_data_helper = GeoDataHelper()

    # Imposto i path del file
    geo_data_helper.set_tree_path(tree_path.name)
    geo_data_helper.set_neighborhood_path(neighborhood_path.name)

    # Creo CSV con la lista dei nomi e il numero di alberi
    geo_data_helper.create_tree_categories_csv('csv/' + tree_path.stem + '_categories')

    # Creo CSV con la lista dei circoscrizioni e il numero di alberi
    geo_data_helper.create_tree_neighborhoods_csv(5, 'csv/' + tree_path.stem + '_neighborhoods')
