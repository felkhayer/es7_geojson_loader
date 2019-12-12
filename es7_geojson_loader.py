#!/usr/bin/env python3

import os.path as path
import json
import urllib3
import argparse

"""
###############################################################################
# Author:   Florian El-Khayer <florian dot elkhayer at gmail dot com>
#
###############################################################################
# Copyright (c) 2019, Florian El-Khayer <florian dot elkhayer at gmail dot com>
#
# Permission is hereby granted, free of charge, to any person obtaining a
# copy of this software and associated documentation files (the "Software"),
# to deal in the Software without restriction, including without limitation
# the rights to use, copy, modify, merge, publish, distribute, sublicense,
# and/or sell copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included
# in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
# OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
# THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
# DEALINGS IN THE SOFTWARE.
###############################################################################
"""

DESC="""
Chargement massif de fichier geojson dans Elasticsearch 7+.
l'index est automatiquement cree si il n'existe pas.
"""
DEFAULT_INDEX_NAME='nom du fichier sans extension'

parser = argparse.ArgumentParser(description=DESC, formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument('geojson', help='chemin du fichier geojson à charger.')
parser.add_argument('-e', '--elastic_host', help="url d'elasticsearch", default="http://localhost:9200")
parser.add_argument('-i', '--index_name', help="nom de l'index ou charger le fichier", default=DEFAULT_INDEX_NAME)
parser.add_argument('-p', '--properties_name', help="nom du champ des proprietes", default='properties')
parser.add_argument('-g', '--geometry_name', help="nom du champ des geometries", default='geometry')
parser.add_argument('-o', '--overwrite_index', help="supprime l'index si il existe avant le chargement.", action="store_true")
parser.add_argument('-b', '--bulk_size', type=int, help="taille des 'lots' à indexer (en nombre d'élèments)", default=5000)
args = parser.parse_args()

geojson_file_path=args.geojson
if not path.exists(geojson_file_path):
    raise Exception("Fichier '{geojson_file_path}' invalide.")
es_host=args.elastic_host
# nom de l'index à charger. par defaut => le nom du fichier sans extension
index_name=(args.index_name if args.index_name != DEFAULT_INDEX_NAME else path.basename(path.splitext(geojson_file_path)[0]))
properties_field_name=args.properties_name
geometry_field_name=args.geometry_name
overwrite_index=args.overwrite_index
bulk_size=args.bulk_size

HTTP = urllib3.PoolManager()

def delete_index(es_host, index_name):
    DELETE_IGNORED_STATUS=[404] # 404=> index non trouvé

    delete_index_request = HTTP.request(method='DELETE', url="%s/%s" % (es_host, index_name))
    if delete_index_request.status != 200:
        if delete_index_request.status not in DELETE_IGNORED_STATUS:
            raise Exception(create_index_request.status, delete_index_request.data)

def create_index(es_host, index_name, index_mapping):
    CREATE_IGNORED_STATUS=[400] # 400=> index deja present

    encoded_data=json.dumps(index_mapping).encode('utf-8')
    create_index_request = HTTP.request(method='PUT', url="%s/%s" % (es_host, index_name), body=encoded_data, headers={'Content-Type': 'application/json'})
    if create_index_request.status != 200:
        if create_index_request.status not in CREATE_IGNORED_STATUS:
            raise Exception(create_index_request.status, create_index_request.data)

def bulk_loading(es_host, index_name, data_list, bulk_size=500):
    def chunk_data(data_list, bulk_size):
        """
        Decoupe le flux en list de 'bulk_size' elements ou moins.
        """
        chunk=list()
        for (i, d) in enumerate(data_list, 1):
            chunk.append(d)
            if (i % bulk_size) == 0:
                yield chunk
                chunk=list()
        yield chunk
    
    def prepare_bulk_load_data(chunk):
        """ajoute les actions 'index' du bulk load et transform en json les elements."""
        ACTION=json.dumps({"index" : {}})
        for element in chunk:
            yield ACTION
            yield json.dumps(element)
        yield ''
    
    total_elements=0
    for chunk in chunk_data(data_list, bulk_size):
        prepared_bulk = '\n'.join(prepare_bulk_load_data(chunk)).encode('utf-8')
        bulk_load_request = HTTP.request(method='POST', url="%s/%s/_bulk" % (es_host, index_name), body=prepared_bulk, headers={'Content-Type': 'application/json'})
        if bulk_load_request.status != 200:
            raise Exception(bulk_load_request.status, bulk_load_request.data)
        total_elements=total_elements+len(chunk)
        print("%d éléments chargés" % total_elements)

"""
Chargement du fichier geojson en memoire.
TODO: lecture en flux du geojson (les autres fonctions utilisés prennent des iterateurs en parametre)
    --> la taille du flux ne pourra plus etre fourni
"""
file_handler = open(geojson_file_path, 'r')
geojson_dict = json.load(file_handler)
print("Nombre d'elements : %d" % len(geojson_dict['features']))
file_handler.close()

"""
Adaptation du format geojson en "document" pour elasticsearch.
(definition de la transformation via un generateur pour reduire l'utilisation memoire)
"""
features = ({**_f[properties_field_name], **{geometry_field_name:_f[geometry_field_name]}} for _f in geojson_dict['features'])

"""
creation de l'index avec un mapping specifiant le champ de coordonnées, puis chargement massif.
DONE: suppression de l'index sur option (si -overwrite)
"""
index_mapping = {
    "mappings": {
        "properties": {
            geometry_field_name: {
                "type": "geo_shape"
            }
        }
    }
}
if overwrite_index:
    delete_index(es_host, index_name)
create_index(es_host, index_name, index_mapping)
bulk_loading(es_host, index_name, features, bulk_size)
