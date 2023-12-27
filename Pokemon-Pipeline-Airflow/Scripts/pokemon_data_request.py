import json 
import pandas as pd
import requests
from time import sleep

def get_pokemons(n_pokemon=None):

    '''
    n_poke: The amount of Pokemons that will be fetched if none will fetch all
    '''
    url = 'https://pokeapi.co/api/v2/pokemon/'
    if n_pokemon is None:
        n_pokemon = json.loads(requests.get(url).content)['count']

    params = {'limit': n_pokemon}
    status_code = 0
    while status_code != 200:

        response = requests.get(url, params=params)
        status_code = response.status_code

        if status_code == 200:
            pokemons = []
            for pokemon in range(1, n_pokemon+1):
                try:
                    end = f'https://pokeapi.co/api/v2/pokemon/{pokemon}/'
                    response = requests.get(end)
                    data = json.loads(response.content)
                    pokemons.append(({
                                    'name': data['name'],
                                    'type': [x['type']['name'] for x in data['types']],
                                    'weight_kg': round(data['weight'] / 10),
                                    'heigth_cm': data['height'] * 100 / 10,
                                    'stats': {x['stat']['name']: x['base_stat'] for x in  data['stats']},
                                    'moves': [move['move']['name'] for move in data['moves']],
                                    'total_moves': len(data['moves']),
                                    'hidden_ability': {x['ability']['name']: x['is_hidden'] for x in  data['abilities']},
                                    }))
                except:
                    pass
            break
        elif status_code in (502, 503, 504):
            print(f'Trying again status_code: {status_code} message: {response.text}')
            sleep(30)



    return pokemons

path = '/opt/airflow/datasets/raw/'
dataframe = pd.DataFrame(get_pokemons()).to_csv(f'{path}raw_pokemon_dataset.csv', index=False, encoding='utf-8')