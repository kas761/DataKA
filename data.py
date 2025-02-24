import pandas as pd

red_wine = pd.read_csv("winequality-red.csv")
white_wine = pd.read_csv("winequality-white.csv")

red_wine['wine_type'] = 'red'
white_wine['wine_type'] = 'white'

wine = pd.concat([red_wine, white_wine], ignore_index=True)

print(wine.head())
