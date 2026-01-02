#!/bin/bash

# set up env files from default file
[ -f ".env" ] || cp default.env .env

# check if ~/.Makefile exists, if not create an empty one
[ -f "$HOME/.Makefile" ] || touch "$HOME/.Makefile"

echo "This script sets up all system config/files required to run the project"