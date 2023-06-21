#include <iostream>
#include <random>
#include <string>
#include <utility>
#include <vector>
#include <iomanip>

#define DIMENSIONALITY 2

struct Point
{
	double componenets[DIMENSIONALITY];

	Point operator-() const
	{
		Point n;
		for(unsigned i = 0; i < DIMENSIONALITY; i++)
			n.componenets[i] = -componenets[i];
		
		return n;
	}

	Point operator+(const Point &a) const
	{
		Point n;
		for(unsigned i = 0; i < DIMENSIONALITY; i++)
			n.componenets[i] = componenets[i] + a.componenets[i];
		
		return n;
	}
};

typedef std::pair<Point, std::vector<std::normal_distribution<>>> CentroidGenerator;

int main(int argl, char *argv[])
{
	// Paramaeters
	const unsigned k = std::stoul(argv[1]);
	const unsigned samples = std::stoul(argv[2]);

	// Centroids & their generators
	std::vector<CentroidGenerator> centroids;
	
	// Seed with a real random value, if available
	std::random_device r;

	// Generators
	std::mt19937 gen_cen(0xcafebabe + 0xdeadbeaf + 7 - 1);
	std::mt19937 gen(r());
	std::uniform_real_distribution<> centroids_random_variable(-150.0, +200.0);
	std::uniform_real_distribution<> shape_random_variable(10, 23);

	std::cout << std::setprecision(7);

	// Generate random centroids
	for(unsigned i = 0; i < k; i++) {
		Point c;
		std::vector<std::normal_distribution<>> generators;

		// Generate centorid and its random vector
		for(unsigned i = 0; i < DIMENSIONALITY; i++)
		{
			c.componenets[i] = centroids_random_variable(gen_cen);

			// Using a different standard dev. for each random vector's component changes the cluster's shape!
			generators.push_back(std::normal_distribution<>(c.componenets[i], shape_random_variable(gen_cen)));
		}

		centroids.push_back(std::make_pair(c, generators));

		std::clog << "centroid " << i << ',';
		for(unsigned j = 0; j < DIMENSIONALITY; j++)
			std::clog << c.componenets[j] << (j != DIMENSIONALITY - 1 ? ',' : '\n');
		std::clog.flush();
	}

	// Generate data
	for(unsigned long i = 0; i < samples; i++) {
		CentroidGenerator& cg = centroids[i % k];

		std::cout << (i%k) << ',';
		for(unsigned i = 0; i < DIMENSIONALITY; i++)
			std::cout << cg.second[i](gen) << (i != DIMENSIONALITY - 1 ? ',' : '\n');
	}

	return 0;
}
