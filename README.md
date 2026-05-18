# TP — Découverte de Spark et limites de Pandas sur des volumes massifs

## Objectifs pédagogiques

À la fin de ce TP, vous serez capables de :

* Comprendre les limites d’un traitement classique avec Pandas.
* Déployer un cluster Spark avec Docker Compose.
* Manipuler des données JSONL avec Pandas et Spark.
* Observer les différences de comportement entre un traitement local et un traitement distribué.
* Comprendre l’intérêt du calcul distribué.
* Répondre à des questions d’analyse et de compréhension sur la scalabilité.

---

# Partie 1 — Mise en place de l’environnement Spark

## 1.1 Architecture utilisée

Dans ce TP, nous allons utiliser :

* X Spark Master
* X Spark Workers
* X environnement Jupyter avec PySpark

L’ensemble sera déployé avec Docker Compose. 

**Question:** Remplacer le X par le nombe adéquat a la lecture de docker compose. 

---

## 1.2 Docker Compose

Créer un fichier `docker-compose.yml` contenant la configuration suivante.

```yaml
# docker-compose.yml - Spark cluster with 1 master and 3 workers
version: "3.8"

services:
  
  spark-master:
    image: apache/spark:3.5.1
    container_name: spark-master
    ports:
      - "8080:8080"
      - "7077:7077"
      - "4040:4040"
    environment:
      - SPARK_MODE=master
    command: /opt/spark/bin/spark-class org.apache.spark.deploy.master.Master
    volumes:
      - spark-data:/opt/spark/work-dir

  spark-worker-1:
    image: apache/spark:3.5.1
    container_name: spark-worker-1
    depends_on:
      - spark-master
    environment:
      - SPARK_MODE=worker
      - SPARK_MASTER_URL=spark://spark-master:7077
      - SPARK_WORKER_MEMORY=2g
      - SPARK_WORKER_CORES=2
    command: >
      /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker
      spark://spark-master:7077
    volumes:
      - spark-data:/opt/spark/work-dir

  spark-worker-2:
    image: apache/spark:3.5.1
    container_name: spark-worker-2
    depends_on:
      - spark-master
    environment:
      - SPARK_MODE=worker
      - SPARK_MASTER_URL=spark://spark-master:7077
      - SPARK_WORKER_MEMORY=2g
      - SPARK_WORKER_CORES=2
    command: >
      /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker
      spark://spark-master:7077
    volumes:
      - spark-data:/opt/spark/work-dir

  spark-worker-3:
    image: apache/spark:3.5.1
    container_name: spark-worker-3
    depends_on:
      - spark-master
    environment:
      - SPARK_MODE=worker
      - SPARK_MASTER_URL=spark://spark-master:7077
      - SPARK_WORKER_MEMORY=2g
      - SPARK_WORKER_CORES=2
    command: >
      /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker
      spark://spark-master:7077
    volumes:
      - spark-data:/opt/spark/work-dir

  jupyter:
    image: jupyter/pyspark-notebook:latest
    container_name: spark-jupyter
    ports:
      - "8888:8888"
    environment:
      - JUPYTER_ENABLE_LAB=yes
    volumes:
      - ./work:/home/jovyan/work
    command: start-notebook.sh --NotebookApp.token='spark'

volumes:
  spark-data:
```

---

## 1.3 Démarrage du cluster

Lancer la commande suivante :

```bash
docker compose up -d
```

Vérifier ensuite les conteneurs :

```bash
docker ps
```
**Question:** Qu'obtenevez  vous ?

---

## 1.4 Vérification du cluster



### Interface Jupyter

Ouvrir :

```text
http://localhost:8888
```

Token :

```text
spark
```

Créer un notebook Python.

---

# Partie 2 — Découverte des données

## 2.1 Jeu de données utilisé

Nous allons utiliser des données Amazon au format JSONL.

### Petit jeu de données

Vous pouvez utiliser un petit fichier de `Gift_Cards.jsonl` pour tester le bon fonctionnement depuis ce lien :

```text
https://mcauleylab.ucsd.edu/public_datasets/data/amazon_2023/raw/meta_categories/meta_Gift_Cards.jsonl.gz

```

---

### Grand jeu de données (> 1 Go)

Télécharger le fichier suivant :

```text
https://mcauleylab.ucsd.edu/public_datasets/data/amazon_2023/raw/meta_categories/meta_Automotive.jsonl.gz
```

Décompresser le fichier :

```bash
gunzip meta_Automotive.jsonl.gz
```

### Transfert dans jupyter 

Dans la machine hôte, placer les fichiers :

```text
notebook
```

**Question** : Expliquez, du point de vue architectural, où se situe chaque élément (L'hôte, les fichiers, spark, le master ...)

---

# Partie 3 — Premier test avec Pandas

## 3.1 Import des bibliothèques

```python
import os
import pandas as pd
```

---

## 3.2 Lecture du petit jeu de données

```python
pandas_df = pd.read_json(
    'work/meta_Gift_Cards.jsonl',
    lines=True
)
```

---

## 3.3 Analyse du volume de données

```python
file_path = r"work/meta_Gift_Cards.jsonl"

size_gb = os.path.getsize(file_path) / (1024 ** 3)

print(f'Taille du fichier : {size_gb:.2f} GB')
```

Afficher les premières lignes.

```python
print(pandas_df.head())
```


---

## Questions

### Question 1


Où les données sont-elles chargées lors de l’utilisation de Pandas ?
---

### Question 2

Quel est le principal risque lorsque la taille des données augmente fortement ?

---



# Partie 4 — Test Pandas sur un très gros volume

## 4.1 Taille du fichier Automotive

```python
file_path = r"work/meta_Automotive.jsonl"

size_gb = os.path.getsize(file_path) / (1024 ** 3)

print(f'Taille du fichier : {size_gb:.2f} GB')
```

---

## 4.2 Chargement avec Pandas

```python
pandas_df = pd.read_json(
    'work/meta_Automotive.jsonl',
    lines=True
)
```

---

## Vos Observations

Selon votre machine :

> Completer moi ....

---

## Questions d’analyse

### Question 4

Pourquoi Pandas rencontre-t-il cette situation?

---

### Question 5

Expliquez le lien entre :

* taille du fichier
* mémoire RAM disponible
* architecture de Pandas

---

### Question 6

Pourquoi cette approche devient-elle difficilement scalable ?

---

### Question 7

Que faudrait-il faire si le fichier faisait 100 Go ?

![genius](https://imgflip.com/s/meme/Roll-Safe-Think-About-It.jpg)

---

# Partie 5 — Introduction à Spark

## 5.1 Création de la session Spark

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("spark://spark-master:7077") \
    .appName('AutomotiveData') \
    .getOrCreate()

print(f'The Spark version is {spark.version}')
```

---

## 5.2 Lecture des données avec Spark

```python
spark_df = spark.read.json(
    'work/meta_Automotive.jsonl'
)
```

---

## 5.3 Affichage du schéma

```python
spark_df.printSchema()
```

---

## 5.4 Affichage des données

```python
spark_df.show(5)
```

---

# Partie 6 — Premières manipulations Spark

## 6.1 Nombre total de lignes

```python
spark_df.count()
```

---

## 6.2 Sélection de colonnes

```python
spark_df.select(
    'title',
    'average_rating'
).show(10)
```

---

## 6.3 Filtrage

```python
spark_df.filter(
    spark_df.average_rating > 4.5
).show(10)
```

---

## 6.4 Agrégation

```python
from pyspark.sql.functions import avg

spark_df.select(
    avg('average_rating')
).show()
```

---

# Partie 7 — Comprendre ce que fait Spark

## Observation de l’interface Spark

Ouvrir :

```text
http://localhost:4040
```

Observer :

* les jobs
* les stages
* les tasks
* la répartition du calcul

---

## Questions

### Question 8

Quelle différence fondamentale existe-t-il entre Pandas et Spark ?

---

### Question 9

Pourquoi Spark peut-il traiter des volumes beaucoup plus importants ?

---

### Question 10

Quel est le rôle des workers dans Spark ?

---

### Question 11

Pourquoi Spark est-il adapté au Big Data ?

---

### Question 12

Quels sont les inconvénients possibles de Spark ?

<!-- ---

# Partie 8 — Comparaison Pandas vs Spark

Compléter le tableau suivant.

| Critère                      | Pandas | Spark |
| ---------------------------- | ------ | ----- |
| Chargement mémoire           |        |       |
| Calcul distribué             |        |       |
| Scalabilité                  |        |       |
| Simplicité d’utilisation     |        |       |
| Performances petits fichiers |        |       |
| Performances gros volumes    |        |       |
| Tolérance aux pannes         |        |       |
| Infrastructure nécessaire    |        |       |

---

# Partie 9 — Analyse critique

## Question 13

Dans quels cas Pandas reste-t-il un très bon choix ?

---

## Question 14

Dans quels cas Spark devient-il indispensable ?

---

## Question 15

Expliquez pourquoi le calcul distribué devient essentiel dans les architectures Big Data modernes.

---

## Question 16

Pourquoi l’utilisation de plusieurs machines peut-elle être plus efficace qu’une seule machine très puissante ?

---

# Partie 10 — Bonus

## Bonus 1 — Mesure de temps

Comparer le temps d’exécution entre Pandas et Spark.

### Pandas

```python
import time

start = time.time()

pandas_df = pd.read_json(
    'work/meta_Gift_Cards.jsonl',
    lines=True
)

end = time.time()

print(end - start)
```

---

### Spark

```python
import time

start = time.time()

spark_df = spark.read.json(
    'work/meta_Gift_Cards.jsonl'
)

spark_df.count()

end = time.time()

print(end - start)
```

---

## Bonus 2 — Étude des partitions

Afficher le nombre de partitions.

```python
spark_df.rdd.getNumPartitions()
```

---

## Bonus 3 — Modification du nombre de partitions

```python
repartitioned_df = spark_df.repartition(8)

repartitioned_df.rdd.getNumPartitions()
```

---

# Conclusion

Dans ce TP, vous avez :

* déployé un cluster Spark
* observé les limites de Pandas
* manipulé des données massives
* découvert le calcul distribué
* compris les enjeux de scalabilité

Spark apporte une solution adaptée aux traitements Big Data grâce à :

* la distribution des données
* le parallélisme
* la tolérance aux pannes
* la capacité à traiter des volumes massifs

À l’inverse, Pandas reste extrêmement efficace pour :

* les petits volumes
* l’exploration rapide
* le prototypage
* les analyses locales -->
