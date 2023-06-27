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

static const double schifo[5][2] = {
	{-100,150},
	{0,0},
	{59,120},
	{100,-100},
	{175,-10}
};

int main(int argl, char *argv[])
{
	// Paramaeters
	const unsigned k = std::stoul(argv[1]);
	const unsigned samples = std::stoul(argv[2]);
	const unsigned noise = std::min((unsigned long)(samples * 0.08), 1000ul);

	// Centroids & their generators
	std::vector<CentroidGenerator> centroids;
	
	// Seed with a real random value, if available
	std::random_device r;

	// Generators
	std::mt19937 gen_cen(argl >= 4 ? std::stoull(argv[3]) : 0xcafebabe + 0xdeadbeaf + 7);
	std::mt19937 gen(r());
	std::uniform_real_distribution<> centroids_random_variable(-150.0, +200.0);
	std::uniform_real_distribution<> noise_g(-150.0-30.0, 200.0+30.0);
	std::uniform_real_distribution<> shape_random_variable(8, 26);

	std::cout << std::setprecision(7);

	// Generate random centroids
	for(unsigned f = 0; f < k; f++) {
		Point c;
		std::vector<std::normal_distribution<>> generators;

		// Generate centorid and its random vector
		for(unsigned i = 0; i < DIMENSIONALITY; i++)
		{
			c.componenets[i] = schifo[f][i];

			// Using a different standard dev. for each random vector's component changes the cluster's shape!
			generators.push_back(std::normal_distribution<>(c.componenets[i], shape_random_variable(gen_cen)));
		}

		centroids.push_back(std::make_pair(c, generators));

		std::clog << "centroid " << f << ',';
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

	// Generate noise
	for(unsigned long i = 0; i < noise; i++) {
		std::cout << "N,";
		for(unsigned i = 0; i < DIMENSIONALITY; i++)
			std::cout << noise_g(gen) << (i != DIMENSIONALITY - 1 ? ',' : '\n');
	}

	return 0;
}
