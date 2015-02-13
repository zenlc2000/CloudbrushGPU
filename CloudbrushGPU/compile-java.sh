#!/bin/bash

ant jar
java -jar toolchain/pack.jar -mainjar dist/cloudbrush.jar -lib lib/ -destjar dist/cloudbrush-packed.jar
