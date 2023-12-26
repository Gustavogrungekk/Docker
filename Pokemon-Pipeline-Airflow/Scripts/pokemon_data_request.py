#===================================================================================================================
# Extract Transform Load (ETL) STEP
# Author: Gustavo Barreto
# Language: Python
# Date 26/12/2023
#===================================================================================================================

import json 
import pandas as pd
import requests
from time import sleep

def get_poke(url, n_poke=int):
    '''
    url: None the less the api endpoint we're requesting from
    n_poke: The amount of Pokemons that will be fetched
    '''
    params = {'limit': n_poke+1}
    status_code = 0
    while status_code != 200:

        response = requests.get(url, params=params)
        status_code = response.status_code

        if status_code == 200:
            data = json.loads(response.content)['results']
            break
        elif status_code in (502, 503, 504):
            print(f'Trying again status_code: {status_code} message: {response.text}')
            sleep(30)


    poke_data  = []
    for url in data:
        req = requests.get(url['url'])
        infos = json.loads(req.content)

        # Getting status info
        weight = infos['weight'] / 10 # Divided by 10 so we can get the exact weight in KG

        poke_data.append({'name': url['name'],
                        'type': [x['type']['name'] for x in infos['types']],
                        'moves': [x['move']['name'] for x in infos['moves']],
                        'weigth_kg': weight,
                        'height_cm': infos['height'] / 10,
                        'total_moves': len([x for x in infos['moves']]),
                        'stats': {stat['stat']['name']: stat['base_stat'] for stat in infos['stats']}
                        })

    # Creating the Data Frame
    df = pd.DataFrame(poke_data)
    return df

# calling the function and storing the data
path = '/opt/airflow/datasets/raw/'
get_poke('https://pokeapi.co/api/v2/pokemon/', n_poke=1017).to_csv(f'{path}raw_pokemon_dataset.csv', index=False, encoding='utf-8')