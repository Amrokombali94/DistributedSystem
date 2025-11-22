FROM ubuntu:22.04

RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y \
    build-essential cmake git pkg-config ca-certificates \
    libasio-dev python3 \
    libcurl4-openssl-dev \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY CMakeLists.txt /app/CMakeLists.txt
COPY src /app/src
COPY include /app/include

RUN echo "=== /app layout ===" && ls -R /app

RUN cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -DCMAKE_VERBOSE_MAKEFILE=ON \
 && cmake --build build -- -j

ENV PORT=8000
EXPOSE 8000

# run the server by default
CMD ["/app/build/rest-api"]
