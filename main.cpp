#include "mapreduce.hpp"
#include <unordered_set>
#include <cmath>

using namespace std;
using namespace mapreduce;

// Calculates length of the binary representation of a number in essentially O(1)
// Respectfully stolen from StackOverflow (linked in credits)
int binaryLength(int n) {
    int i = 0; // the minimum number of bits required.
    if (n >= 0x7FFF) {n >>= 16; i += 16;}
    if (n >= 0x7F) {n >>= 8; i += 8;}
    if (n >= 0x7) {n >>= 4; i += 4;}
    if (n >= 0x3) {n >>= 2; i += 2;}
    if (n >= 0x1) {i += 1;}
    return i;
}

// Calculates whether a number is a perfect power by using
// a binary search, respectfully stolen as well from StackOverflow (credited)
vector<pair<int, int>> findPowers(int *value, int reducer_count) {
    int x = *value;

    vector<pair<int, int>> pairs;

    if (x == 1) {
        for (int i = 2; i < reducer_count + 2; i++) {
            pairs.emplace_back(i, x);
        }
        return pairs;
    }

    if (x == 0) return pairs;

    // found this on the internet don't @
    int length = binaryLength(x) + 1;
    long lowA, highA, midA, ab;
    for (int b = 2; b < reducer_count + 2; b++) {
        lowA = 1;
        highA = 1 << (length / b + 1);
        while (lowA < highA - 1) {
            midA = (lowA + highA) >> 1;
            ab = (long) pow(midA, b);
            if (ab > x) {
                highA = midA;
            } else if (ab < x) {
                lowA = midA;
            } else {
                pairs.emplace_back(b, x);
                break;
            }
        }
    }
    return pairs;
}

// Convert to set and return size to count unique values
int countUnique(vector<int> values) {
    unordered_set<int> set = unordered_set(values.begin(), values.end());
    return (int) set.size();
}

// Every reducer takes care of its "power" and since the count starts from 2, we add 2
int id(int id) {
    return id + 2;
}

int toInt(string s) {
    return stoi(s);
}

int main(int argc, char** argv) {
    MapReduce<int, int> mapReduce;

    if (argc < 4) {
        cout << "Usage: " << argv[0] << " mapper_count reducerCount directory_file\n";
        return 0;
    }

    int mapper_count = stoi(argv[1]);
    int reducer_count = stoi(argv[2]);
    string location = argv[3];

    mapReduce.start(location, mapper_count, reducer_count, &findPowers, &toInt, &countUnique, &id, &to_string);
    return 0;
}