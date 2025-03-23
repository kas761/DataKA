import pandas as pd

red_wine = pd.read_csv("winequality-red.csv", delimiter=";")
white_wine = pd.read_csv("winequality-white.csv", delimiter=";")
#wine = pd.read_csv("winequality.csv", delimiter=";")

red_wine["wine_type"] = 'red'
white_wine["wine_type"] = 'white'

wine = pd.concat([red_wine, white_wine], ignore_index=True)


high_quality_wine = wine[wine["quality"] >= 7]
low_quality_wine = wine[wine['quality'] <= 4]

high_average_quality = round(high_quality_wine['quality'].mean(),2)
low_average_quality = round(low_quality_wine['quality'].mean(),2)

print(f"Average quality of high quality wine: {high_average_quality}, average quality of low quality wine: {low_average_quality}")