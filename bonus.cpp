#include "mapreduce.hpp"
#include <unordered_set>
#include <cmath>

using namespace std;
using namespace mapreduce;

vector<pair<int, string>> countChars(string *value, int reducer_count) {
    vector<pair<int, string>> vector;
    vector.emplace_back(rand() % reducer_count, *value + " - " + to_string(value->length()) + "\n");
    return vector;
}


string combineResults(vector<string> values) {
    unordered_set<string> unique(values.begin(), values.end());
    string result;
    for (auto value : unique) {
        result.append(value);
    }
    return result;
}

int id(int id) {
    return id;
}

string toString(string s) {
    return s;
}

int main(int argc, char** argv) {
    MapReduce<int, string> mapReduce;

    if (argc < 4) {
        cout << "Usage: " << argv[0] << " mapper_count reducerCount directory_file\n";
        return 0;
    }

    int mapper_count = stoi(argv[1]);
    int reducer_count = stoi(argv[2]);
    string location = argv[3];

    mapReduce.start(location, mapper_count, reducer_count, &countChars, &toString, &combineResults, &id, &to_string);
    return 0;
}