#!/usr/bin/env r 
library(magrittr)

f <- file('stdin') 
open(f)
data <- readLines(f,warn = FALSE)
close(f)

data <- read.csv(text = data,stringsAsFactors = FALSE)

cat(nrow(data),'\n')
cat(ncol(data),'\n')
rm(data)
