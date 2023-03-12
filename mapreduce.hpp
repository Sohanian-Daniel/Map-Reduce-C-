#ifndef MAPREDUCE_H
#define MAPREDUCE_H
#include <unordered_map>
#include <vector>
#include <fstream>
#include <pthread.h>
#include <iostream>

using namespace std;
#define MAP_FUNC(name) vector<pair<K, V>> (*name)(V *value, int reducer_count)
#define RED_FUNC(name) V (*name)(vector<V> values)
#define REDUCER_ID(name) K (*name)(int id)
#define CONVERT_FUNC(name) V (*name)(string s)
#define TO_STRING(name) string (*name)(K reducer_id)

namespace mapreduce {
    class FileManager {
    public:
        unordered_map<string, bool> files;
        pthread_mutex_t mutex{};

        FileManager() {
            pthread_mutex_init(&mutex, nullptr);
        }

        ~FileManager() {
            pthread_mutex_destroy(&mutex);
        }

        string *nextFile() {
            // Multiple threads have access to the FileManager, only one should receive a certain file
            if (_allOpen) return nullptr;
            pthread_mutex_lock(&mutex);
            for (auto &entry: files) {
                if (!entry.second) {
                    entry.second = true;
                    pthread_mutex_unlock(&mutex);
                    return (string *) &entry.first;
                }
            }
            _allOpen = true;
            pthread_mutex_unlock(&mutex);
            return nullptr;
        };

        bool allOpen() {
            bool value;
            pthread_mutex_lock(&mutex);
            value = _allOpen;
            pthread_mutex_unlock(&mutex);
            return value;
        }
    private:
        bool _allOpen = false;
    };

    template<typename K, typename V>
    struct mapper_args {
        int id;
        int reducerCount;
        MAP_FUNC(mapFunc);
        CONVERT_FUNC(convert);
        FileManager *fileManager;
        vector<unordered_map<K, vector<V>>> *partialValues;
        pthread_barrier_t *barrier;
    };

    template<typename K, typename V>
    struct reducer_args {
        int id;
        RED_FUNC(reduceFunc);
        REDUCER_ID(IdToKey);
        TO_STRING(keyToString);
        vector<unordered_map<K, vector<V>>> *partialValues;
        pthread_barrier_t *barrier;
    };

    template<typename K, typename V>
    class MapReduce {
    private:
        // Hashtable equivalent
        vector<unordered_map<K, vector<V>>> partialValues;
        FileManager fileManager;
        pthread_barrier_t barrier{};

        static void *mapper(void *args) {
            // Extract Arguments
            mapper_args<K, V> arg = *(mapper_args<K, V> *) args;

            // Required to Read from Files
            fstream file;
            string text;

            // Number of lines in File
            int n;

            // Used to store Value read from File
            V value;

            // Mapper's Partial Values (Dictionary)
            unordered_map<K, vector<V>> map;

            // A List of Results to be inserted in the Dictionary from the Map Function
            vector<pair<K, V>> pairs;

            // While there are files available to be opened
            while (!arg.fileManager->allOpen()) {

                // Get next file from file-manager
                string *nextFile = arg.fileManager->nextFile();
                if (nextFile != nullptr) {
                    file.open(*nextFile);
                } else {
                    break;
                }

                // Read number of lines
                file >> n;
                // Ignore the end-line
                file.ignore();

                // Read all Values from File
                for(int i = 0; i < n; i++) {
                    // Read Values line by line and convert them to Value type
                    getline(file, text);
                    value = arg.convert(text);

                    // Obtain all pairings from the user inputted Map function
                    pairs = arg.mapFunc(&value, arg.reducerCount);

                    // Insert all Pairs into the Dictionary
                    for (auto pair : pairs) {
                        if (map.count(pair.first) == 0) {
                            // Add new entry in the Map at Pair.first
                            map.emplace(pair.first, vector<V>());

                            // Add the value Pair.second to the List at Pair.first
                            map.at(pair.first).push_back(pair.second);
                        } else {
                            // Add the value Pair.second to the List at Pair.first
                            map.at(pair.first).push_back(pair.second);
                        }
                    }
                }
                file.close();
            }

            // Insert Dictionary in the Vector of partial results at the Mapper's ID
            // All Mappers have a unique ID therefore there are no overlaps
            arg.partialValues->at(arg.id) = map;

            // Finished executing, once all Mappers gather here the Reducers will start
            pthread_barrier_wait(arg.barrier);
            return nullptr;
        }

        static void *reducer(void *args) {
            reducer_args<K, V> arg = *(reducer_args<K, V> *) args;

            // Wait for Mappers to finish executing
            pthread_barrier_wait(arg.barrier);

            // Obtain the Key the Reducer is responsible for
            K key = arg.IdToKey(arg.id);

            // Format output file based on keyToString function provided by user
            ofstream file("out" + arg.keyToString(key) + ".txt");

            // Aggregate all Partial Values for the Key the Reducer is responsible for in a Vector
            vector<V> v;
            for (auto entry : * (arg.partialValues)) {
                v.insert(v.end(), entry[key].begin(), entry[key].end());
            }

            // Apply the Reduce Function to the Vector of Values
            V value = arg.reduceFunc(v);

            // Output Result to File
            file << value;
            file.close();
            return nullptr;
        }
    public:
        void start(const string& directory, int mapper_count, int reducer_count,
                   MAP_FUNC(mapFunc), CONVERT_FUNC(toKey),
                   RED_FUNC(reduceFunc), REDUCER_ID(IdToKey), TO_STRING(keyToString)) {
            pthread_t threads[mapper_count + reducer_count];
            mapper_args<K, V> mapperArgs[mapper_count];
            reducer_args<K, V> reducerArgs[reducer_count];

            pthread_barrier_init(&barrier, nullptr, mapper_count + reducer_count);

            // initialise partialValues with mapper_count entries
            partialValues.resize(mapper_count);

            // Init arguments for mappers and reducers
            for (int i = 0; i < mapper_count; i++) {
                mapperArgs[i].id = i;
                mapperArgs[i].reducerCount = reducer_count;
                mapperArgs[i].mapFunc = mapFunc;
                mapperArgs[i].convert = toKey;
                mapperArgs[i].fileManager = &fileManager;
                mapperArgs[i].partialValues = &partialValues;
                mapperArgs[i].barrier = &barrier;
            }

            for (int i = 0; i < reducer_count; i++) {
                reducerArgs[i].id = i;
                reducerArgs[i].reduceFunc = reduceFunc;
                reducerArgs[i].IdToKey = IdToKey;
                reducerArgs[i].keyToString = keyToString;
                reducerArgs[i].partialValues = &partialValues;
                reducerArgs[i].barrier = &barrier;
            }

            // Init fileManager with all files required for parse
            fstream file(directory);
            int n;
            string text;
            file >> n;
            file.ignore();
            for (int i = 0; i < n; i++) {
                getline(file, text);
                fileManager.files[text] = false;
            }

            // Start all Mappers and Reducers
            int r;
            for (int i = 0; i < mapper_count + reducer_count; i++) {
                if (i < mapper_count) {
                    r = pthread_create(&threads[i], nullptr, mapper, (void *) &mapperArgs[i]);
                    if (r) {
                        cout << "ERROR CREATING MAPPER\n";
                        exit(-1);
                    }
                } else {
                    r = pthread_create(&threads[i], nullptr, reducer, (void *) &reducerArgs[i - mapper_count]);
                    if (r) {
                        cout << "ERROR CREATING REDUCER\n";
                        exit(-1);
                    }
                }
            }

            // Join all Mappers and Reducers
            void *status;
            for(int i = 0; i < mapper_count + reducer_count; i++) {
                r = pthread_join(threads[i], &status);

                if (r) {
                    cout << "ERROR JOINING THREADS\n";
                    exit(-1);
                }
            }
        }
    };
}
#endif