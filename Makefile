build: tema1

all: tema1 bonus

tema1:
	g++ main.cpp -lpthread -Wall -Werror -O0 -std=c++17 -o tema1
bonus:
	g++ bonus.cpp -lpthread -Wall -Werror -O0 -std=c++17 -o bonus
clean:
	rm tema1
	rm bonus