Ces différents notebooks permettent de mettre en forme les données cadastres en ligne sur : https://cadastre.data.gouv.fr/dvf

Les notebook permetttent de préparer un jeu de données permettant d'entraîner différents algos ( réseau de neurones, random forest ). 

Il est possible de devoir faire quelques modifications sur les fichiers pour obtenir un résultat satisfaisant.

Soit un total de 4 fichiers ( de 2014 à 2017 ). Dans le code, ces fichiers s'appellent full-2014.csv, full-2015.csv, etc.

Ce sont les seules fichiers sources utiles. Tous ces notebooks ne sont pas utiles ( j'en ai utilisé pour faire des tests ) . 

Je vais donner ci dessous les plus importants à travers un exemple.

Par exemple, pour entrainer un lstm, il faut éxécuter ces notebooks dans l'ordre:

- mise_en_forme_foncier_2014_annee-mois.ipynb ( toujours le premier à éxécuter et pour chaque année. Il permet de nettoyer chaque fichier source )
- join_immo_par_mois.ipynb ( pour joindre les fichiers de chaque année )
- lstm.ipynb ( pour entrainer un lstm )

Dans le dossier mise_en_forme, on retrouve un script composé de deux fichiers qui se chargent de faire la mise en forme et la jointure de tous les fichiers sources. Ce qui est plus rapide que de répéter la mise en forme pour chaque année dans un notebook.


