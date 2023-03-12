# Map-Reduce-C-

Done as part of an Parallel and Distributed Algorithm course in late 2022.

This project implements a simplified Map-Reduce programming model in C++, skipping over the Shuffle operation. Done using pthreads.

## Example implementations
1. main.cpp counts the amount of powers of (thread_id + 2) for a given set of data.
2. bonus.cpp counts the length of each line in the input data.

Usage :
- make [all/main/bonus] 
- ./[main/bonus] [mapper_count] [reducer_count] [input_file_location]

The input file has the number of data files on the first line and their location on the following ones.

## Usage
Create a MapReduce<K, V> instance and call the .start method with the following arguments:

- The input file location
- The amount of Mapper Threads.
- The amount of Reducer Threads.
- A "Mapping" function with the following signature: vector<pair<K, V>> (*func_name)(V *value, int reducer_count)
  - The Mapper thread will call this function for every element it receives, it returns all the Key-Value pairs that were processed for the particular value "value".
  - This way, we can return multiple results from a single value and control which reducer threads will pick up our results.
- A conversion function that makes the conversion from Key to String (so we can name the output files properly) and from String to Value for reading from a file.
  You could for example implement a function that reads Graphs from a text file.
- A "Reduction" function with the following signature: V (*func_name)(vector<V> values)
  - The Reducer thread will call this function over the list of values it is responsible for.
- Finally, a function that determines which Reducer is responsible for which dataset based on its ID, K (*func_name)(int id)

The .start method handles output files internally.

## Potential Improvements
- Use native C++ threads implementation instead of pthreads.
- Somehow clean up the process of starting up the whole thing, it seems very convoluted right now.
