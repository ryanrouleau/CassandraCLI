all:
	g++ -Wall -std=c++11 -o cli cmds.cpp cli.cpp -lcassandra
clean:
	rm -f 
