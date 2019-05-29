import pandas as pd


def prepare_foncier_dataset(annee):
    foncier = pd.read_csv(
        '/home/yopla/Dropbox/asi/stage/prediction_prix_immobilier/data/foncier/full_' + annee + '.csv',
        sep=',',
        dtype={'code_commune': str})

    immo = foncier
    cols = ['id_mutation', 'date_mutation', 'numero_disposition',
            'nature_mutation', 'valeur_fonciere',
            'code_postal', 'code_commune', 'nom_commune', 'code_departement', 'id_parcelle',
            'code_type_local', 'type_local',
            'surface_reelle_bati', 'nombre_pieces_principales',
            'code_nature_culture', 'nature_culture',
            'surface_terrain', 'longitude', 'latitude']

    immo = immo[cols]

    immo = immo[immo.type_local.notnull()]

    immo = immo[immo.code_type_local != 3]

    immo = immo[immo.code_type_local != 4]

    immo = immo[immo.valeur_fonciere.notnull()]

    immo = immo[immo.surface_reelle_bati.notnull()]

    immo = immo.groupby('id_mutation').first().sort_values(by=['id_mutation'], ascending=False)

    immoGroup = immo
    immoGroup.reset_index(inplace=True)

    cols_to_keep = ["code_commune", "nom_commune", "type_local", 'code_type_local', "nombre_pieces_principales", "surface_reelle_bati",
                    "valeur_fonciere", "id_mutation"]

    immoGroup = immoGroup[cols_to_keep]
    immoGroup = immoGroup.groupby(["code_commune", "nom_commune", "type_local", 'code_type_local', "nombre_pieces_principales"]).agg(
        {'surface_reelle_bati': 'mean', 'valeur_fonciere': 'mean', 'id_mutation': 'size'}).rename(
        columns={'id_mutation': 'nb_mutation'}).reset_index()

    immoGroup['id_bien'] = immoGroup["code_commune"] + "_" + immoGroup["code_type_local"].astype(
        int).astype(str) + "_" + immoGroup["nombre_pieces_principales"].astype(int).astype(str)

    immoGroup.to_csv(
        '/home/yopla/Dropbox/asi/stage/prediction_prix_immobilier/data/foncier/immobilier_' + annee + '_clean.csv',
        index=False)

def prepare_foncier_dataset_by_month(annee):
    foncier = pd.read_csv(
        '/home/yopla/Dropbox/asi/stage/prediction_prix_immobilier/data/foncier/full_' + annee + '.csv',
        sep=',',
        dtype={'code_commune': str,'code_postal' : str})

    immo = foncier
    cols = ['id_mutation', 'date_mutation', 'numero_disposition',
            'nature_mutation', 'valeur_fonciere',
            'code_postal', 'code_commune', 'nom_commune', 'code_departement', 'id_parcelle',
            'code_type_local', 'type_local',
            'surface_reelle_bati', 'nombre_pieces_principales',
            'code_nature_culture', 'nature_culture',
            'surface_terrain', 'longitude', 'latitude']

    immo = immo[cols]

    immo = immo[immo.type_local.notnull()]

    immo = immo[immo.code_type_local != 3]

    immo = immo[immo.code_type_local != 4]

    immo = immo[immo.valeur_fonciere.notnull()]

    immo = immo[immo.surface_reelle_bati.notnull()]

    immo = immo.groupby('id_mutation').first().sort_values(by=['id_mutation'], ascending=False)

    import datetime
    immo['year'] = immo.date_mutation.map(lambda x: datetime.datetime.strptime(x, '%Y-%m-%d').year)
    immo['month'] = immo.date_mutation.map(lambda x: datetime.datetime.strptime(x, '%Y-%m-%d').month)

    print("p1");

    immo_2   = immo

    cols_to_keep = ["code_commune","code_postal","nom_commune","code_type_local", "type_local","nombre_pieces_principales","surface_reelle_bati","valeur_fonciere","month","year"]

    immo_2 = immo_2[cols_to_keep]

    immo_2 = immo_2.reset_index()

    immo_2['id_bien'] = immo_2["code_commune"] + "_" + immo_2["code_type_local"].astype(int).astype(
        str) + "_" + immo_2["nombre_pieces_principales"].astype(int).astype(str)

    cols_bien = ['code_commune','code_postal', 'nom_commune', 'nombre_pieces_principales', 'code_type_local', 'type_local', 'id_bien']

    print("p2");

    bien = immo_2[cols_bien].drop_duplicates()

    bien.to_csv(
        '/home/yopla/Dropbox/asi/stage/prediction_prix_immobilier/data/foncier/bien_' + annee + '.csv',
        index=False)

    cols_bien.pop()

    immo_2 = immo_2.drop(columns=cols_bien).drop(columns='id_mutation')

    id_bien = pd.DataFrame(data={'id_bien': immo_2['id_bien'].drop_duplicates()})

    mois = immo_2[['month', 'year']].drop_duplicates().sort_values(by="month")

    print("p3");

    id_bien["key"] = 0
    mois["key"] = 0
    bien_par_mois = id_bien.merge(mois, how='left', on='key').drop(columns="key")

    immoGroup = immo_2

    immoGroup = immoGroup.groupby(["id_bien", "month", "year"]).agg(
        {'surface_reelle_bati': ['mean', 'count'], 'valeur_fonciere': 'mean'}).reset_index()

    immoGroup.columns = ['id_bien', 'month', 'year', 'surface', 'nb_mutation', 'valeur']

    immoFinal = bien_par_mois.merge(immoGroup, how='left', on=['id_bien', 'month', 'year'])

    print("p4");

    immoFinal.fillna(0, inplace=True)

    immoFinal.to_csv(
        '/home/yopla/Dropbox/asi/stage/prediction_prix_immobilier/data/foncier/immobilier_' + annee + '_clean_month.csv',
        index=False)


def join_all_year_foncier(years):
    data_immo = {}
    for year in years:
        tempo = pd.read_csv('/home/yopla/Dropbox/asi/stage/prediction_prix_immobilier/data/foncier/immobilier_' + year + '_clean.csv', sep=',',
                            dtype={'code_commune': str})
        tempo.rename(columns={'surface_reelle_bati': 'surface_' + year, 'valeur_fonciere': 'valeur_' + year,
                              'nb_mutation': 'nb_mutation_' + year}, inplace=True)
        data_immo[year] = tempo

    cols_to_keep = ['code_commune', 'nom_commune', 'type_local', 'code_type_local', 'nombre_pieces_principales']

    immo_by_year = ""
    for i in range(len(years)):
        immo = data_immo[year]
        year = years[i]

        if i != 0:
            for col in cols_to_keep:
                immo.rename(columns={col: col + '_' + year}, inplace=True)

        if i == 0:
            immo_by_year = immo
        else:
            immo_by_year = pd.merge(immo_by_year, immo, on='id_bien', how='outer')


    col_names = immo_by_year.columns.values
    col_names = list(set(col_names) - set(cols_to_keep))

    from collections import defaultdict
    cols_to_merge = defaultdict(list)

    for col_key in cols_to_keep:
        for col_value in col_names:
            start_with = col_value.startswith(col_key)
            if start_with:
                cols_to_merge[col_key].append(col_value)

    for final_col, inter_cols in cols_to_merge.items():
        for inter_col in inter_cols:
            immo_by_year.loc[~immo_by_year[inter_col].isna(), final_col] = immo_by_year.loc[
                ~immo_by_year[inter_col].isna(), inter_col]
            immo_by_year.drop(columns=[inter_col], inplace=True)

    immo_by_year.fillna(0, inplace=True)

    first_cols = ['id_bien', 'code_commune', 'nom_commune', 'type_local', 'code_type_local',
                  'nombre_pieces_principales']
    col_names = immo_by_year.columns.values
    last_cols = list(set(col_names) - set(first_cols))
    last_cols.sort()
    new_col_names = first_cols + last_cols
    print(new_col_names)

    immo_by_year = immo_by_year[new_col_names]

    immo_by_year.to_csv('/home/yopla/Dropbox/asi/stage/prediction_prix_immobilier/data/join/immo_by_year.csv', index=False)

def prepare_pop_dataset(annee):
    pop = pd.read_csv(
        '/home/yopla/Dropbox/asi/stage/prediction_prix_immobilier/data/population/pop_commune_' + annee + '.csv',
        sep=',', dtype={'Code commune': str})
    pop.columns = ['code_region', 'nom_region', 'code_departement',
                   'code_arrondissement', 'code_canton', 'code_commune',
                   'nom_commune', 'population_municipale',
                   'population_comptee_a_part', 'population_totale']

    pop["code_commune_complet"] = pop["code_departement"] + pop["code_commune"].astype(str)

    cols = ['code_commune_complet', 'population_totale']
    pop = pop[cols]

    pop.rename(columns={'code_commune_complet': 'code_commune'}, inplace=True)

    pop.to_csv(
        '/home/yopla/Dropbox/asi/stage/prediction_prix_immobilier/data/population/pop_commune_' + annee + '_clean.csv',
        index=False)


def prepare_pop_ensemble_dataset(annee):
    pop = pd.read_csv(
        '/home/yopla/Dropbox/asi/stage/prediction_prix_immobilier/data/csp/base-ic-evol-struct-pop-' + annee + '.csv',
        sep=',', dtype={'COM': str})

    end_annee = annee[2:]

    cols = ['COM', 'P14_POP', 'P14_POP0002', 'P14_POP0305', 'P14_POP0610',
            'P14_POP1117', 'P14_POP1824', 'P14_POP2539', 'P14_POP4054',
            'P14_POP5564', 'P14_POP6579', 'P14_POP80P', 'P14_POP0014',
            'P14_POP1529', 'P14_POP3044', 'P14_POP4559', 'P14_POP6074',
            'P14_POP75P', 'P14_POP0019', 'P14_POP2064', 'P14_POP65P',
            'P14_POPH', 'P14_H0014', 'P14_H1529', 'P14_H3044', 'P14_H4559',
            'P14_H6074', 'P14_H75P', 'P14_H0019', 'P14_H2064', 'P14_H65P',
            'P14_POPF', 'P14_F0014', 'P14_F1529', 'P14_F3044', 'P14_F4559',
            'P14_F6074', 'P14_F75P', 'P14_F0019', 'P14_F2064', 'P14_F65P',
            'C14_POP15P', 'C14_POP15P_CS1', 'C14_POP15P_CS2', 'C14_POP15P_CS3',
            'C14_POP15P_CS4', 'C14_POP15P_CS5', 'C14_POP15P_CS6',
            'C14_POP15P_CS7', 'C14_POP15P_CS8', 'C14_H15P', 'C14_H15P_CS1',
            'C14_H15P_CS2', 'C14_H15P_CS3', 'C14_H15P_CS4', 'C14_H15P_CS5',
            'C14_H15P_CS6', 'C14_H15P_CS7', 'C14_H15P_CS8', 'C14_F15P',
            'C14_F15P_CS1', 'C14_F15P_CS2', 'C14_F15P_CS3', 'C14_F15P_CS4',
            'C14_F15P_CS5', 'C14_F15P_CS6', 'C14_F15P_CS7', 'C14_F15P_CS8',
            'P14_POP_FR', 'P14_POP_ETR', 'P14_POP_IMM', 'P14_PMEN',
            'P14_PHORMEN']

    cols = map(lambda x: x.replace('C14', 'C' + end_annee).replace('P14', 'P' + end_annee), cols)

    pop = pop[cols]

    pop = pop.rename(columns={'COM': 'code_commune'})

    pop.columns = pop.columns.map(lambda x: x.lower())

    popcommune = pop.groupby('code_commune').sum().reset_index()

    popcommune.to_csv(
        '/home/yopla/Dropbox/asi/stage/prediction_prix_immobilier/data/csp/ensemble_pop_' + annee + '.csv',
        index=False)


def join_pop_foncier(annee):
    pop = pd.read_csv('/home/yopla/Dropbox/asi/stage/prediction_prix_immobilier/data/population/pop_commune_' + annee + '_clean.csv',
                      sep=',', dtype={'code_commune': str})

    immo = pd.read_csv('/home/yopla/Dropbox/asi/stage/prediction_prix_immobilier/data/foncier/immobilier_' + annee + '_clean.csv', sep=',',
                       dtype={'code_commune': str})

    join_pop_immo_ = pd.merge(immo, pop, on='code_commune', how='inner')

    join_pop_immo_.to_csv('/home/yopla/Dropbox/asi/stage/prediction_prix_immobilier/data/join/join_immo_pop_' + annee + '.csv',
                          index=False)

def join_ensemble_pop_foncier(annee):
    pop = pd.read_csv('/home/yopla/Dropbox/asi/stage/prediction_prix_immobilier/data/csp/ensemble_pop_' + annee + '.csv', sep=',', dtype={'code_commune': str})

    immo = pd.read_csv('/home/yopla/Dropbox/asi/stage/prediction_prix_immobilier/data/foncier/immobilier_' + annee + '_clean.csv', sep=',', dtype={'code_commune': str})

    join_pop_immo_ = pd.merge(immo, pop, on='code_commune', how='inner')

    join_pop_immo_.to_csv('/home/yopla/Dropbox/asi/stage/prediction_prix_immobilier/data/join/join_immo_ens_pop_' + annee + '.csv', index=False)