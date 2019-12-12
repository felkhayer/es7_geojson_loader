# es7_geojson_loader
Elasticsearch 7+ geojson bulk loader

```
> ./es7_geojson_loader.py -h
usage: es7_geojson_loader.py [-h] [-e ELASTIC_HOST] [-i INDEX_NAME]
                             [-p PROPERTIES_NAME] [-g GEOMETRY_NAME] [-o]
                             [-b BULK_SIZE]
                             geojson

Chargement massif de fichier geojson dans Elasticsearch 7+. l'index est
automatiquement cree si il n'existe pas.

positional arguments:
  geojson               chemin du fichier geojson à charger.

optional arguments:
  -h, --help            show this help message and exit
  -e ELASTIC_HOST, --elastic_host ELASTIC_HOST
                        url d'elasticsearch (default: http://localhost:9200)
  -i INDEX_NAME, --index_name INDEX_NAME
                        nom de l'index ou charger le fichier (default: nom du
                        fichier sans extension)
  -p PROPERTIES_NAME, --properties_name PROPERTIES_NAME
                        nom du champ des proprietes (default: properties)
  -g GEOMETRY_NAME, --geometry_name GEOMETRY_NAME
                        nom du champ des geometries (default: geometry)
  -o, --overwrite_index
                        supprime l'index si il existe avant le chargement.
                        (default: False)
  -b BULK_SIZE, --bulk_size BULK_SIZE
                        taille des 'lots' à indexer (en nombre d'élèments)
                        (default: 5000)
```
