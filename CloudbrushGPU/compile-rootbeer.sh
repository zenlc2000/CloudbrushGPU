#!/bin/bash

if [ ! -d "build/rootbeer" ]; then
	mkdir -p "build/rootbeer"
fi

ant compile-rootbeer

ant jar-rootbeer

file="dist/gpu.jar"
if [ -f "$file" ];
then
	printf "\n\nRunning Rootbeer on $file.\n\n"
	java -jar toolchain/Rootbeer-1.2.4.jar dist/gpu.jar dist/Cloudbrush-GPU-tmp.jar -64bit
else
	printf "\n\nERROR: $file not found. Exiting with errors.\n\n"
fi

file="dist/Cloudbrush-GPU-tmp.jar"
if [ -f "$file" ];
then
	printf "\n\nStarting pack.\n\n"
	java -jar toolchain/pack.jar -mainjar dist/CloudbrushGPU.jar -directory lib -libjar dist/Cloudbrush-GPU-tmp.jar -destjar CloudbrushGPU-GPU.jar
else
	printf "\n\nERROR: $file not found. Exiting with errors.\n\n"
fi



rm build/rootbeer/*.java
