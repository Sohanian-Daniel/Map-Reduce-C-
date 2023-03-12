build: main

all: main bonus

tema1:
	g++ main.cpp -lpthread -Wall -Werror -O0 -std=c++17 -o main
bonus:
	g++ bonus.cpp -lpthread -Wall -Werror -O0 -std=c++17 -o bonus
clean:
	rm main
	rm bonus
