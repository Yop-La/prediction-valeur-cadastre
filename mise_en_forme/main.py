import pandas as pd
from functions import *



years = ['2014','2015','2016','2017']

# prepation des données foncières
for year in years:
    prepare_foncier_dataset_by_month(year)
    print(year)


#
# # join des foncier data
# join_all_year_foncier(years)
#
# for year in years:
#     if int(year) <= 2016:
#         prepare_pop_dataset(year) # préparation des données population
#     # join_pop_foncier(annee) # la jointure entre pop et foncier
#
#
#     # De population ensemble ensuite
#
#     if int(year) <= 2015:
#         prepare_pop_ensemble_dataset(year) # préparation des données ensemble population
#         # join_ensemble_pop_foncier(annee) # la jointure entre pop ensemble et foncier
#
#
