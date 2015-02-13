#!/bin/bash

#ant compile-java

if [ ! -d "build/classes/rootbeer" ]; then
	mkdir build/classes/rootbeer
fi

cp src/Brush/*Kernel.java build/rootbeer/.
ant jar-java
