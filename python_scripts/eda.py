import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import warnings
warnings.filterwarnings('ignore')

df = pd.read_json('data/data.json')
# print(df.sample(5))

# print(df.loc[:, "driving_licence"])

# print(df.info())

# print(df.loc[:, "number_of_vacancies"])

print(df["workplace_address"].str["municipality"])

# print(df.describe())
