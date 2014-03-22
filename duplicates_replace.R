################################################################################
## schmingo's R snippets                                                      ##
##                                                                            ##
## Unzip files and remove all duplicate lines for each containing file.       ##
## Corrected files will be replaced.                                          ##
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
                       sep = ",",
                       na.strings = "nan")
  
  ## Get unique lines
  to_check.unique <- unique(to_check)
  
  nrow(to_check)
  nrow(to_check.unique)
  
  if (nrow(to_check)!=nrow(to_check.unique)) ## only replace changed files
    write.csv(to_check.unique,
              file = c,
              row.names = FALSE,
              quote = FALSE,
              na = "nan")
}
stopCluster(cl)


################################################################################
### Zip files ##################################################################
################################################################################

### Won't work in win8.1


# list.folder <- list.dirs(path = zip.folder, 
#                          recursive = FALSE)
# 
# list.folder
# 
# # f <- list.folder[1]
# 
# registerDoParallel(cl <- makeCluster(ncores))
# foreach(f = list.folder, .packages = lib) %dopar% {
#   
#   list.zipfiles <- list.files(path = f,
#                               recursive = TRUE)
#   
#   zip(paste0(f,"_checked",".zip"), 
#       list.zipfiles, 
#       zip = "zip")
# }
# stopCluster(cl)
