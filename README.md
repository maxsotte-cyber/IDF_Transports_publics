# Île-de-France — Transports publics

Un collecteur qui observe le fonctionnement du réseau de transport francilien et en garde l'historique : **état des lignes, retards, suppressions, perturbations, affluence et disponibilité Vélib'**. Les sources publient ces informations en temps réel mais ne les archivent pas ; ce dépôt les historise pour pouvoir les analyser dans le temps.

La collecte tourne toute seule sur les serveurs de GitHub (GitHub Actions) et les relevés sont enregistrés dans le dossier [`data/`](data) sous forme de CSV. L'historique commence le **5 novembre 2026**.

Un seul fichier Python, sans dépendance à installer (Python 3.9 ou plus).

## Ce qui est mesuré

| Thème | Détail |
|---|---|
| **Lignes** | Pour chaque ligne de métro, RER, Transilien, tram, câble et bus : nombre de rames en circulation, courses prévues et supprimées, retard moyen et médian, part des passages avec plus de 5 min de retard, intervalle entre deux passages, état de la ligne. |
| **Perturbations** | Toutes les perturbations en cours : ligne concernée, gravité, cause, intitulé, début et fin prévus. |
| **Affluence** | Entrées estimées par heure et part de l'heure de pointe, calculées à partir des validations du réseau ferré, selon le type de jour (semaine, samedi, dimanche, vacances, jours fériés). |
| **Vélib'** | Vélos mécaniques et électriques, places libres, stations vides ou pleines, taux de remplissage, pour l'ensemble du réseau et station par station. |
| **Qualité de la collecte** | À chaque relevé, une série de contrôles (collecte réussie, couverture de chaque mode, courses en double ou non mises à jour, retards aberrants écartés, lignes absentes du référentiel) classés *ok*, *attention* ou *alerte*. |

## Sources

| Source | Producteur | Utilisation |
|---|---|---|
| [API PRIM](https://prim.iledefrance-mobilites.fr) (horaires temps réel, perturbations, prochains passages, itinéraires) | Île-de-France Mobilités | État des lignes et perturbations. Nécessite une clé gratuite. |
| [Portail open data IDFM](https://data.iledefrance-mobilites.fr) (référentiel des lignes, arrêts, validations) | Île-de-France Mobilités | Noms et familles des lignes, affluence |
| [Ponctualité mensuelle Transilien](https://ressources.data.sncf.com) | SNCF | Ponctualité de référence |
| Flux GBFS [Vélib' Métropole](https://www.velib-metropole.fr) | Vélib' Métropole | Disponibilité des stations |
| [Jours fériés](https://calendrier.api.gouv.fr) et [calendrier scolaire](https://data.education.gouv.fr) | État | Type de jour pour l'affluence |

## Les données

| Fichier | Contenu |
|---|---|
| `data/AAAA-MM/lignes.csv` | Une ligne par ligne de transport et par relevé |
| `data/AAAA-MM/bus_agrege.csv` | Le réseau de bus résumé en une ligne par relevé |
| `data/AAAA-MM/perturbations.csv` | Les perturbations en cours à chaque relevé |
| `data/AAAA-MM/velib_global.csv` | Les chiffres clés du réseau Vélib' |
| `data/AAAA-MM/qualite.csv` | Les contrôles de qualité de chaque collecte |
| `data/AAAA-MM/collectes.csv` | Le journal des collectes (sources jointes, durée, erreurs) |
| `data/velib_stations/AAAA-MM-JJ.csv` | Le détail station par station |

## Utilisation

### Collecte automatique (GitHub Actions)

Le fichier [`.github/workflows/collecte.yml`](.github/workflows/collecte.yml) lance une collecte programmée toutes les 10 minutes (GitHub peut espacer les passages aux heures chargées), puis enregistre les CSV dans le dépôt. La clé PRIM est lue dans le secret `PRIM_API_KEY` du dépôt (*Settings → Secrets and variables → Actions*).

### Tableau de bord local

```bash
python idf_transports_live.py           # tableau de bord sur http://localhost:8765
python idf_transports_live.py --once    # une seule collecte, puis arrêt
```

En mode local, le script collecte en tâche de fond, stocke l'historique dans une base SQLite (`idf_historique.db`) et sert une interface web : état du réseau en direct, perturbations, Vélib', affluence, historique, prochains passages et itinéraires. Il expose aussi des liens CSV fixes (`http://localhost:8765/export/lignes.csv`, etc.) pour brancher Excel ou un outil de visualisation.

L'interface (`idf_transports_live.html`, à placer dans le même dossier que le script) n'est pas encore publiée dans ce dépôt.
