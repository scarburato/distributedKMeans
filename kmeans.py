import numpy as np
from sklearn.cluster import KMeans

# Caricamento dei dati dal file CSV
data = np.loadtxt('samples/test1.csv', delimiter=',', usecols=(1, 2))

# Numero di cluster desiderato
num_clusters = 5

# Creazione di un oggetto KMeans per ottenere i centroidi iniziali
kmeans_init = KMeans(n_clusters=num_clusters, init='random', n_init=1)
kmeans_init.fit(data)

# Esecuzione di un solo passo dell'algoritmo K-means per ottenere i centroidi iniziali
initial_centroids = kmeans_init.cluster_centers_

# Esecuzione dell'algoritmo K-means completo per ottenere i centroidi finali
kmeans = KMeans(n_clusters=num_clusters, init=initial_centroids, n_init=1)
kmeans.fit(data)

# Ottenere i centroidi finali
final_centroids = kmeans.cluster_centers_

# Stampa dei centroidi iniziali e finali
print("Centroidi iniziali:")
print(initial_centroids)
print("Centroidi finali:")
print(final_centroids)
