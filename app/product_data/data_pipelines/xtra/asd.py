
import pandas as pd

df = pd.read_json('../data_samples/champ_data.json')

unique_markets = df['market'].unique()
print(unique_markets)