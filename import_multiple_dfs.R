cat("\014")
################################################################################
##  
##  Imports multiple dataframes from multiple files and renames them by
##  splitting their filename
##  
##  Version: 2014-08-15
##  
################################################################################
##
##  Copyright (C) 2014 Simon Schlauss (sschlauss@gmail.com)
##
##
##  This file is part of BiFoRe.
##  
##  BiFoRe is free software: you can redistribute it and/or modify
##  it under the terms of the GNU General Public License as published by
##  the Free Software Foundation, either version 3 of the License, or
##  (at your option) any later version.
##  
##  BiFoRe is distributed in the hope that it will be useful,
##  but WITHOUT ANY WARRANTY; without even the implied warranty of
##  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
##  GNU General Public License for more details.
##  
##  You should have received a copy of the GNU General Public License
##  along with BiFoRe.  If not, see <http://www.gnu.org/licenses/>.
##  
################################################################################

## Clear workspace
rm(list = ls(all = TRUE))

## Required libraries
lib <- c("reshape")

lapply(lib, function(...) library(..., character.only = TRUE))

## Set working directory
# setwd("/home/schmingo/Daten/")
setwd("D:/")


### Set filepaths ##############################################################

path.csv <- "Dropbox/Code/bifore/src/csv/kili/"


### Import data ################################################################

## List files
files <- list.files(pattern = "kappa.csv",
                    recursive = TRUE,
                    full.names = FALSE,
                    include.dirs = FALSE)

files


## Import and rename each dataframe
for(i in files) {
  
  ## Import csv
  data.raw <- read.csv2(i,
                        dec = ",",
                        header = TRUE,
                        stringsAsFactors = FALSE)
  
  ## Fetch filename and save important parts
  tmp.name <- strsplit(i, split = "_")
  tmp.name <- paste("data",tmp.name[[1]][2], tmp.name [[1]][3], sep = "_")
  
  ## Assign new df name
  assign(tmp.name, data.frame(data.raw))
  
  ## Remove old df
  data.raw <- NULL
  
}
