#!/bin/bash

hadoop fs -mkdir -p party
hadoop fs -mkdir -p party-agreement
hadoop fs -mkdir -p agreement

hadoop fs -put src/test/data/party.txt party
hadoop fs -put src/test/data/party-agreement.txt party-agreement
hadoop fs -put src/test/data/agreement.txt agreement
