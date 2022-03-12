import config
import requests

auth = requests.auth.HTTPBasicAuth(config.CLIENT_ID,config.SECRET_KEY)

data = {'grant_type':'password',
        'username': 'Zazzi_Bazazzi',
        'password':'Kuli@lm@32!'
}

headers = {'User-Agent':'MyAPI/0.0.1'}

res = requests.post('https://www.reddit.com/api/v1/access_token',auth=auth, data=data, headers=headers)

TOKEN = res.json()['access_token']

headers = {**headers,'Authorization': f"bearer {TOKEN}"}

res = requests.get('https://oauth.reddit.com/api/v1/me',headers=headers)

res = requests.get('https://oauth.reddit.com/r/python/hot',headers=headers)
print(res.json())