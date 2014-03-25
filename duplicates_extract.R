################################################################################
## schmingo's R snippets                                                      ##
##                                                                            ##
## Unzip files and remove all duplicate lines for each containing file.       ##
## Corrected files will be moved to a separate folder.                        ##
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
corrected.folder <- "D:/BEXIS/corrected/"


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
                       pattern = ".txt|.dat",
                       full.names = TRUE,
                       recursive = TRUE)
list.fls


c <- list.fls[1]  ## use for testing

registerDoParallel(cl <- makeCluster(ncores))
foreach(c = list.fls, .packages = lib) %dopar% {
  
  ## Read file
  to_check <- read.csv(c,
                       quote = "",
                       header = TRUE,
                       sep = ",",
                       na.strings = "nan")
  
  ## Get corrected lines
  to_check.unique <- unique(to_check)
  
  nrow(to_check)
  nrow(to_check.unique)
  
  if (nrow(to_check)!=nrow(to_check.unique)) ## only replace changed files
    write.csv(to_check.unique,
              file = paste0(c, ".corrected"),
              row.names = FALSE,
              quote = FALSE,
              na = "nan")
}
stopCluster(cl)


################################################################################
### Move corrected files #######################################################
################################################################################

## List corrected files
list.corrected <- list.files(path = zip.folder,
                             pattern = "txt.corrected|dat.corrected",
                             full.names = TRUE,
                             recursive = TRUE)
list.corrected

u <- list.corrected[1]

## Renaming function
func.file.rename <- function(from, to) {
  todir <- dirname(to)
  if (!isTRUE(file.info(todir)$isdir)) dir.create(todir, recursive=TRUE)
  file.rename(from = from,  to = to)
}


registerDoParallel(cl <- makeCluster(ncores))
foreach (u = list.corrected, .packages = lib) %dopar% {
  
  b.file <- unlist(strsplit(dirname(u), "/"))
  
  func.file.rename(from = u,
                   to = paste(corrected.folder,
                              b.file[6],
                              basename(u),sep = "/"))
}
stopCluster(cl)


################################################################################
### Remove .corrected ##########################################################
################################################################################

## List corrected files
list.corrected2 <- list.files(path = corrected.folder,
                              pattern = ".corrected",
                              full.names = TRUE,
                              recursive = TRUE)
list.corrected2

u2 <- list.corrected[1]

## Rename corrected files
registerDoParallel(cl <- makeCluster(ncores))
foreach (u2 = list.corrected2, .packages = lib) %dopar% {
  
  file.rename(u2,
              paste0(substr(u2, 1, nchar(u2)-14), ".dat")
  )
}
stopCluster(cl)
