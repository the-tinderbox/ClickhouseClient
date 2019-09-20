CC=g++

INSTALLBINDIR := ../../bin
OS_NAME := $(shell uname -s | tr A-Z a-z)
EXECUTABLE := "ccat_$(OS_NAME)"

all: ccat

ccat: ccat.cpp
	@echo "Compiling $<..."; $(CC) ccat.cpp -o $(EXECUTABLE)

clean:
	rm -rf ../../bin/ccat

install:
	@echo "Installing $(EXECUTABLE)..."; cp $(EXECUTABLE) $(INSTALLBINDIR)
