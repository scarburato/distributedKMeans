import sys
import numpy as np
import matplotlib.pyplot as plt

def generate_cluster_data(num_points, num_clusters, dimension):
    np.random.seed(0xcafebabe)
    cluster_centers = np.random.rand(num_clusters, dimension) * 100  # Coordinate dei centri dei cluster
    print(cluster_centers)
    points_per_cluster = num_points // num_clusters  # Numero di punti per ogni cluster

    data = []
    labels = []

    for cluster_id, center in enumerate(cluster_centers):
        cluster_points = (7-1)*np.random.randn(points_per_cluster, dimension) + np.expand_dims(center, axis=0)
        data.extend(cluster_points)
        labels.extend([cluster_id] * points_per_cluster)

    return np.array(data), np.array(labels)

# Generazione dei dati di prova
num_points = int(sys.argv[1])
num_clusters = int(sys.argv[2])
dimension = 4

data, labels = generate_cluster_data(num_points, num_clusters, dimension)

# Visualizzazione dei dati
fig = plt.figure()
'''ax = fig.add_subplot(111, projection='3d')
ax.scatter(data[:, 0], data[:, 1], data[:, 2], c=labels, cmap='viridis')
ax.set_title("Dati di prova con {} cluster".format(num_clusters))
plt.show()'''

# Salvataggio dei punti su file
output_file = "random_samples.txt" if len(sys.argv) < 4 else sys.argv[3]
np.savetxt(output_file, data, delimiter = ",")
#np.savetxt(str(num_points), data, delimiter = ",")