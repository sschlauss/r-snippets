################################################################################
## schmingo's R snippets                                                      ##
##                                                                            ##
## Unzip a file and check every containing file for duplicate lines           ##
##                                                                            ##
##                                                                            ##
## Author: Simon Schlauss (sschlauss@gmail.com)                               ##
## Version: 2014-03-22                                                        ##
##                                                                            ##
################################################################################

## Clear workspace
rm(list = ls(all = TRUE))


## Required libraries
lib <- c("utils", "doSNOW", "doParallel")
lapply(lib, function(...) require(..., character.only = TRUE))

## Set working directory
setwd("D:/BEXIS/duplicate_test/")

## Set folders
zip.folder <- "D:/BEXIS/duplicate_test/"
# unique.folder <- "D:/BEXIS/duplicate_test/unique/"

## Create new dir
# dir.create(unique.folder)


################################################################################
### Unzip files ################################################################
################################################################################

ncores <- detectCores()-1

list.zip <- list.files(path = zip.folder,
                       pattern = ".zip", 
                       full.names = TRUE)
list.zip

# z = list.zip[1]  ## use for testing

registerDoParallel(cl <- makeCluster(ncores))
foreach(z = list.zip, .packages = lib) %dopar% {
  unzip(z,
        overwrite = TRUE,
        exdir = substr(basename(z),1,nchar(basename(z))-4),
        unzip = "internal")
}
stopCluster(cl)


################################################################################
### Check for duplicates #######################################################
################################################################################

list.fls <- list.files(path = zip.folder,
                       pattern = ".dat",
                       full.names = TRUE,
                       recursive = TRUE)
list.fls


# c <- list.fls[1]  ## use for testing

registerDoParallel(cl <- makeCluster(ncores))
foreach(c = list.fls, .packages = lib) %dopar% {
  
  ## Read file
  to_check <- read.csv(c,
                    quote = "",
                    header = TRUE,
                    sep = ",")
  
  ## Get unique lines
  to_check.unique <- unique(to_check)
  
  nrow(to_check)
  nrow(to_check.unique)
  
  if (nrow(to_check)!=nrow(to_check.unique)) ## only replace changed files
    write.csv(to_check.unique,
              # file = paste0(unique.folder,substr(c, nchar(zip.folder)+2, nchar(c))),  ## doesn't work yet, dir-tree does not exist.
              file = c,
              row.names = FALSE,
              quote = FALSE)
}
stopCluster(cl)
