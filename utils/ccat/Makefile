CC=g++

INSTALLBINDIR := ../../bin
EXECUTABLE := ccat

all: ccat

ccat: ccat.cpp
	@echo "Compiling $<..."; $(CC) ccat.cpp -o ccat

clean:
	rm -rf ../../bin/ccat

install:
	@echo "Installing $(EXECUTABLE)..."; cp $(EXECUTABLE) $(INSTALLBINDIR)
