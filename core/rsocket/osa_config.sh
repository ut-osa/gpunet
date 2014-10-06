#!/bin/bash

if [[ ! -e ./configure ]]; then
	./autogen.sh
fi

./configure --enable-gpunet --prefix=/usr/local
