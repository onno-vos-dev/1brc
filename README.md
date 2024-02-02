[![Build](https://github.com/onno-vos-dev/1brc/actions/workflows/build.yml/badge.svg?branch=main)](https://github.com/onno-vos-dev/1brc/actions/workflows/build.yml)

# 1 Billion Row Challenge

Erlang implementation of the [One Billion Row Challenge](https://github.com/gunnarmorling/1brc)

This is included in the set of Elixir/Erlang implementations discussed here: gunnarmorling/1brc#93.

### Usage
##### Running PropEr test:
```shell
rebar3 as test eunit
rebar3 as test ct
```

###### Running test implementation
First prepare the measurements files as per instructions found [here](https://github.com/gunnarmorling/1brc?tab=readme-ov-file#running-the-challenge)

Then simply run:
```shell
# For Linux
erlc -W src/*erl && time -v erl +SDio 1 +SDPcpu 50 +sbwt none +sbwtdio none -noshell -s brc run "./measurements.txt" -s init stop  2>&1

# For OSX
erlc -W src/*erl && gtime -v erl +SDio 1 +SDPcpu 50 +sbwt none +sbwtdio none -noshell -s brc run "./measurements.txt" -s init stop  2>&1
```

### Results
##### Results for 1BRC
On Mac M1 Pro:
```
Elapsed (wall clock) time (h:mm:ss or m:ss): 0:09.99
```

##### Results for 1BRC with 10k keys
On Mac M1 Pro:
```
Elapsed (wall clock) time (h:mm:ss or m:ss): 0:16.05
```
