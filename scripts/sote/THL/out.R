library(optparse)

option_list <- list(
  make_option("--data_dir",   type="character", default=file.path(getwd(), "test")),
  make_option("--kiel", type="character", default=NULL),
  make_option("--keep", type="character", default=NULL),
  make_option("--fgid", type="character", default=NULL),
  make_option("--IDs", type="character", default=NULL),
  make_option("--kanta", type="character", default=NULL),
  make_option("--output_1", type="character", default=file.path(getwd(), "finngen_R14_kanta_laboratory_responses_2016_internal_1.0.txt.gz")),
  make_option("--etunimitilasto", type="character", default=NULL),
  make_option("--sukunimitilasto", type="character", default=NULL),
  make_option("--output_2", type="character", default=file.path(getwd(), "finngen_R14_kanta_laboratory_responses_freetext_2016_internal_1.0.txt.gz")),
  make_option("--chunk_size", type="integer",  default=10000L),
  make_option("--test",       action="store_true", default=FALSE),
  make_option("--debug",      type="logical",  default=FALSE)
)
opt <- parse_args(OptionParser(option_list=option_list))

if (is.null(opt$kiel)) opt$kiel <- file.path(opt$data_dir, "Yhdistetty_biopankkikieltolista-09-09-2025_FINNGENID.txt")
if (is.null(opt$keep)) opt$keep <- file.path(opt$data_dir, "r14_genotype_samples_from_FG.txt")
if (is.null(opt$fgid)) opt$fgid <- file.path(opt$data_dir, "finngen_pic_and_finngenid_r14.txt")
if (is.null(opt$IDs)) opt$IDs <- file.path(opt$data_dir, "R14_offset_090925.txt")
if (is.null(opt$kanta)) opt$kanta <- file.path(opt$data_dir, "FD_1352_6460_2025_laboratoriovastaukset_2016.csv.gz")
if (is.null(opt$etunimitilasto)) opt$etunimitilasto <- file.path(opt$data_dir, "etunimitilasto-2025-08-13-dvv.xlsx")
if (is.null(opt$sukunimitilasto)) opt$sukunimitilasto <- file.path(opt$data_dir, "sukunimitilasto-2025-08-13-dvv.xlsx")

#Libraries
library(data.table)
library(dplyr)
library(haven)
library(stringr)
library(tidyr)
library(R.utils)
library(readxl)

#Denials
kiel <-  fread(opt$kiel,data.table=FALSE,colClasses="character",header = F)
head(kiel);dim(kiel) # R14: 1908
kiel_ <- kiel$V1 

#Geno qc -list (from FG)
keep <- fread(opt$keep, data.table=FALSE,colClasses="character",header = F) 
dim(keep) #  519870       
#keep<-keep$V1 

denials_in_genoqc <- merge(keep, kiel, by="V1", sort=F)
dim(denials_in_genoqc) #0

#hetu-fgid
fgid <- fread(opt$fgid,data.table=FALSE,colClasses="character",header = T)
head(fgid);dim(fgid) #R14 564907    
colnames(fgid) <- c("HETU","FINNGENID");
head(fgid)

# 
htun2date <- function(ht) paste0(match(substr(ht,7,7),c('+','-','A'))+17,substr(ht,5,6),'-',substr(ht,3,4),'-',substr(ht,1,2))

#offset
IDs <- fread(opt$IDs, data.table=F) 
head(IDs);dim(IDs) # 564907


#####Laboratoriodata 2016


library(readr)
first_chunk <- TRUE
n_chunk     <- 0L
n_total     <- 0L
message("Starting chunked processing of ", opt$kanta)
invisible(read_csv_chunked(
  opt$kanta,
  SideEffectChunkCallback$new(function(kanta, .pos) {
    kanta <- as.data.frame(kanta)
    kanta[] <- lapply(kanta, as.character)
    chunk_input_rows_ <- nrow(kanta)
    if (isTRUE(opt$test) && n_chunk >= 2L) return(invisible(NULL))
    dim(kanta) #12743774      29
    head(kanta)
    names(kanta)
    
    #Check crippled rows
    table(nchar(kanta$hetu), useNA="always")
    #      11
    #12743774, all ok
    
    
    #Join FINNGENID
    kanta <- left_join(kanta, fgid, by=c("hetu"="HETU"))
    head(kanta);dim(kanta) #13376554      30
    table(nchar(kanta$FINNGENID), useNA = 'always')
    
    #filter with genolist
    kanta <- subset(kanta, FINNGENID %in% keep$V1)
    dim(kanta) #11861026      30
    #head(kanta)
    
    # Create Bday
    kanta$SYNTPVM <- htun2date(kanta$hetu)
    
    #remove hetu
    kanta <- select(kanta, -hetu)
    head(kanta);colnames(kanta)
    
    # EVENT_AGE
    kanta$EVENT_AGE <- as.numeric(as.Date(kanta$tutkimusaika,"%Y-%m-%d") - as.Date(kanta$SYNTPVM,"%Y-%m-%d"))/365.24 # DATE
    kanta$EVENT_AGE <- round(kanta$EVENT_AGE,digits=3)
    summary(kanta$EVENT_AGE)
    #Min. 1st Qu.  Median    Mean 3rd Qu.    Max.
    #0.00   49.27   64.20   59.96   73.43  105.66
    #remove over 110 v, if they exist
    #kanta <- filter(kanta, EVENT_AGE <= 110)
    
    # remove SYNTPVM and potilassyntymaaika_pvm
    kanta <- select(kanta, -SYNTPVM, -potilassyntymaaika_pvm) 
    head(kanta);dim(kanta) #11861026      29
    
    #APPROX_EVENT_DAY
    #join offset 
    kanta <- left_join(kanta, IDs, by="FINNGENID") 
    dim(kanta) #11861026       30
    head(kanta) 
    
    ## ## ## ## ##
    
    kanta$APPROX_EVENT_DAY <- as.Date(kanta$tutkimusaika, "%Y-%m-%d") + kanta$offset
    kanta$TIME <- substr(kanta$tutkimusaika, 12, 16)
    
    head(subset(kanta, select=c(FINNGENID, tutkimusaika, APPROX_EVENT_DAY, offset, TIME)))
    
    #remove tutkimusaika, offset, potilas_sukupuoli, and APPROX_EVENT_DAY to character
    kanta <- select(kanta, -tutkimusaika)
    kanta <- select(kanta, -offset)
    kanta <- select(kanta, -potilas_sukupuoli)
    kanta$APPROX_EVENT_DAY <- as.character(kanta$APPROX_EVENT_DAY)
    
    head(kanta);dim(kanta) #11861026       29
    
    #check extra carriage returns 
    # try to remove if necessary
    kanta %>% select(paikallinentutkimusnimike_koodi) %>% filter(., grepl("\\r", paikallinentutkimusnimike_koodi)) -> carrets
    dim(carrets) #0    1
    count(carrets, paikallinentutkimusnimike_koodi, sort=T)
    
    kanta %>% select(paikallinentutkimusnimike_selite) %>% filter(., grepl("\\r", paikallinentutkimusnimike_selite)) -> carrets
    dim(carrets) #1770    1
    count(carrets, paikallinentutkimusnimike_selite, sort=T)
    
    
    #####remove extra line carrets
    
    #function to remove extra line carrets (order important): 
    normalize_newlines <- function(x) {
      # Leave NA:s 
      if (is.factor(x)) x <- as.character(x)
      if (!is.character(x)) x <- as.character(x)
      
      # Stage 1: CRLF -> LF
      x <- gsub("\r\n", "\n", x, fixed = TRUE)
      
      # Stage 2: Remove standalone CRs
      x <- gsub("\r", "", x, fixed = TRUE)
      
      # Stage 3: Merge newlines that remain between fields (if any)
      # For example, newline characters are merged if they appear between fields  
      x <- gsub("(?<=\\S)\n(?=\\S)", " ", x, perl = TRUE)
    
      # Stage 4: Remove extra empty lines from beginning 
      x <- gsub("^\n+", "", x)
    
      # Stage 5: Remove extra newlines at the end of the file
      # This ensures that there are no unnecessary empty lines at the end of the file  
      x <- gsub("\n$", "", x)
      
      return(x)
    }
    
    kanta$paikallinentutkimusnimike_selite <- normalize_newlines(kanta$paikallinentutkimusnimike_selite)
    
    #if you want to check, write out different values:
    #fwrite(subset(kanta, is.na(paikallinentutkimusnimike_selite)==F & duplicated(paikallinentutkimusnimike_selite)==F, select=c("paikallinentutkimusnimike_selite")), file="finngen_R14_kanta_laboratory_responses_paikallinentutkimusnimike_selite_fixed_2016.txt",quote=F,sep="\t",na="NA", append=!first_chunk, col.names=first_chunk, nThread=1, compress="gzip")
    
    #####removing carrets ends
    
    colnames(kanta)
    #DF14
    # [1] "asiakirjaoid_pseudo"              "merkintaoid_pseudo"
    # [3] "entryoid_pseudo"                  "file_name_pseudo"
    # [5] "load_id_pseudo"                   "laboratoriotutkimusoid"
    # [7] "laboratoriotutkimusnimike"        "paikallinentutkimusnimike_koodi"
    # [9] "paikallinentutkimusnimike_selite" "tutkimuskoodistonjarjestelma"
    #[11] "tutkimuksentekotapa"              "tutkimuksennaytelaatu"
    #[13] "tiedonlahde"                      "tutkimuksenlisatieto"
    #[15] "tuloksenvalmistumisaika"          "tutkimusvastauksentila"
    #[17] "tutkimustulosarvo"                "tutkimustulosyksikko"
    #[19] "tutkimustulosteksti"              "tuloksenpoikkeavuus"
    #[21] "viitearvoryhma"                   "viitevalialkuarvo"
    #[23] "viitevalialkuyksikko"             "viitevaliloppuarvo"
    #[25] "viitevaliloppuyksikko"            "FINNGENID"
    #[27] "EVENT_AGE"                        "APPROX_EVENT_DAY"
    #[29] "TIME"
    
    #DF14 new variables: tutkimuksenlisatieto, tutkimustulosteksti
    head(subset(kanta, is.na(tutkimuksenlisatieto)==F, select=c("tutkimuksenlisatieto")))
    #freetext
    head(subset(kanta, is.na(tutkimustulosteksti)==F, select=c("tutkimustulosteksti")))
    #example values: 96*, 138, 0.00, NEGAT, +/-
    
    
    #empties to NA
    kanta[kanta==''] <- NA
    #"NA":s to NA
    kanta[kanta=='NA'] <- NA
    
    neededcols <- c("FINNGENID", "EVENT_AGE", 
                    "APPROX_EVENT_DAY", "TIME", "asiakirjaoid_pseudo", "merkintaoid_pseudo", "entryoid_pseudo", "load_id_pseudo", "file_name_pseudo",
                    "laboratoriotutkimusoid", "laboratoriotutkimusnimike", "paikallinentutkimusnimike_koodi", "paikallinentutkimusnimike_selite", "tutkimuskoodistonjarjestelma",
                    "tiedonlahde", "tutkimusvastauksentila", "tuloksenvalmistumisaika", "tutkimustulosarvo", "tutkimustulosyksikko",
                    "tutkimuksennaytelaatu", "tutkimuksentekotapa", "tuloksenpoikkeavuus", "viitearvoryhma", "viitevalialkuarvo",
                    "viitevalialkuyksikko", "viitevaliloppuarvo", "viitevaliloppuyksikko")
    
    
    neededcols[!neededcols%in%colnames(kanta)]
    neededcols[!colnames(kanta)%in%neededcols] #"tutkimuskoodistonjarjestelma" "tutkimustulosyksikko"
    neededcols[neededcols%in%colnames(kanta)]
    
    #different data to freetext variables
    dim(kanta) #11861026      29
    dim(unique(kanta)) #11859581
    #check, what variables need to be included that combining is possible 
    #combining with "asiakirjaoid_pseudo", "merkintaoid_pseudo", "entryoid_pseudo", "load_id_pseudo", "file_name_pseudo"
    
    #only tutkimustulosteksti is needed from freetext variables, no tutkimuksenlisatieto
    kanta_avo <- select(kanta,"FINNGENID", "EVENT_AGE", 
                    "APPROX_EVENT_DAY", "TIME", "asiakirjaoid_pseudo", "merkintaoid_pseudo", "entryoid_pseudo", "load_id_pseudo", "file_name_pseudo",
                    "tutkimustulosteksti")
    
    dim(kanta_avo) #11861026      10
    
    #choose variables and correct order
    kanta <- select(kanta,"FINNGENID", "EVENT_AGE", 
                    "APPROX_EVENT_DAY", "TIME", "asiakirjaoid_pseudo", "merkintaoid_pseudo", "entryoid_pseudo", "load_id_pseudo", "file_name_pseudo",
                    "laboratoriotutkimusoid", "laboratoriotutkimusnimike", "paikallinentutkimusnimike_koodi", "paikallinentutkimusnimike_selite", "tutkimuskoodistonjarjestelma",
                    "tiedonlahde", "tutkimusvastauksentila", "tuloksenvalmistumisaika", "tutkimustulosarvo", "tutkimustulosyksikko",
                    "tutkimuksennaytelaatu", "tutkimuksentekotapa", "tuloksenpoikkeavuus", "viitearvoryhma", "viitevalialkuarvo",
                    "viitevalialkuyksikko", "viitevaliloppuarvo", "viitevaliloppuyksikko")
    
    #write out
    head(kanta)
    dim(kanta) #11861026       27
    
    denials_in_data <- merge(subset(kanta, select=c(FINNGENID)), kiel, by.x="FINNGENID", by.y="V1", sort=F)
    dim(denials_in_data) #0
    
    #remove also tuloksenvalmistumisaika
    kanta <- select(kanta, -tuloksenvalmistumisaika)
    dim(kanta) #11861026       26
    fwrite(kanta, file=opt$output_1,quote=T,sep="\t",na="NA", append=!first_chunk, col.names=first_chunk, nThread=1, compress="gzip")
    
    
    #These not included: 
    #New in DF14: tutkimuksenlisatieto, tutkimustulosteksti
    #FREETEXT VARIABLES have their own data
    
    
    #kanta <- fread(opt$output_1, data.table=F)
    
    #CHECK VARIABLES - IF OK then to Finngen
    table(nchar(kanta$FINNGENID), useNA = 'always') #all 10, ok
    table(substr(kanta$FINNGENID,1,2), useNA = 'always') #all begins "FG", ok
    
    summary(kanta$EVENT_AGE) 
    
    #
    table(nchar(kanta$APPROX_EVENT_DAY), useNA = 'always')
    table(substr(kanta$APPROX_EVENT_DAY,1,2), useNA = 'always')
    table(substr(kanta$APPROX_EVENT_DAY,1,4), useNA = 'always') #2015-2017, because of approx not only 2016
    
    table(nchar(kanta$tuloksenvalmistumisaika), useNA = 'always') 
    # removed, not in data!
    
    table(nchar(kanta$TIME), useNA = 'always')
    table(substr(kanta$TIME,1,3), useNA = 'always') #ok
    
    table(nchar(kanta$asiakirjaoid_pseudo), useNA = 'always')
    table(nchar(kanta$merkintaoid_pseudo), useNA = 'always')
    table(nchar(kanta$entryoid_pseudo), useNA = 'always')
    table(nchar(kanta$load_id_pseudo), useNA = 'always')
    table(nchar(kanta$file_name_pseudo), useNA = 'always')
    
    
    kanta %>% count(laboratoriotutkimusoid, sort=T) #almost 12 milj different
    table(nchar(kanta$laboratoriotutkimusoid), useNA = 'always')
    kanta %>% count(laboratoriotutkimusnimike, sort=T) #1699 erilaista
    
    kanta %>% count(paikallinentutkimusnimike_koodi, sort=T) #16 851 erilaista, numeerinen koodi?
    kanta %>% count(paikallinentutkimusnimike_selite, sort=T) #10 160 erilaista, tekstimuotoinen koodi?
    
    kanta %>% count(tutkimuskoodistonjarjestelma, sort=T) #406 erilaista
    kanta %>% count(tiedonlahde, sort=T) #NA tai HOIORG
    
    kanta %>% count(tutkimusvastauksentila, sort=T) #F, C, P, NA, here not I
    #kanta %>% count(tuloksenvalmistumisaika, sort=T) #removed
    
    kanta %>% count(tutkimustulosarvo, sort=T) #15 889 erilaista
    summary(kanta$tutkimustulosarvo) #min -xx ja max xxxxxxxx
    kanta %>% count(tutkimustulosyksikko, sort=T) #332 erilaista yksikkoa
    table(kanta$tutkimustulosyksikko, useNA="always")
    
    kanta %>% count(tutkimuksennaytelaatu, sort=T) #21 erilaista, many NA
    table(kanta$tutkimuksennaytelaatu, useNA="always") #OK
    kanta %>% count(tutkimuksentekotapa, sort=T) # 1 tai NA
    kanta %>% count(tuloksenpoikkeavuus, sort=T) #13 erilaista, kaikki OK, tosin yksi B ja yksi <
    kanta %>% count(viitearvoryhma, sort=T) #1561 erilaista viitearvoa, paljon NA:ta
    table(kanta$viitearvoryhma, useNA = 'always')
    
    
    kanta %>% count(viitevalialkuarvo, sort=T)
    summary(kanta$viitevalialkuarvo) #min -x ja max xxxx
    kanta %>% count(viitevalialkuyksikko, sort=T) #ok
    table(kanta$viitevalialkuyksikko, useNA="always") #ok, yksikkoja
    
    kanta %>% count(viitevaliloppuarvo, sort=T)
    summary(kanta$viitevaliloppuarvo) #min -98 ja max 99999
    kanta %>% count(viitevaliloppuyksikko, sort=T)
    table(kanta$viitevaliloppuyksikko, useNA="always") #ok, yksikkoja
    
    #OK!!, Below kanta_avo (kanta freetext)
    
    names(kanta)
    #DF14
    # [1] "FINNGENID"                        "EVENT_AGE"
    # [3] "APPROX_EVENT_DAY"                 "TIME"
    # [5] "asiakirjaoid_pseudo"              "merkintaoid_pseudo"
    # [7] "entryoid_pseudo"                  "load_id_pseudo"
    # [9] "file_name_pseudo"                 "laboratoriotutkimusoid"
    #[11] "laboratoriotutkimusnimike"        "paikallinentutkimusnimike_koodi"
    #[13] "paikallinentutkimusnimike_selite" "tutkimuskoodistonjarjestelma"
    #[15] "tiedonlahde"                      "tutkimusvastauksentila"
    #[17] "tutkimustulosarvo"                "tutkimustulosyksikko"
    #[19] "tutkimuksennaytelaatu"            "tutkimuksentekotapa"
    #[21] "tuloksenpoikkeavuus"              "viitearvoryhma"
    #[23] "viitevalialkuarvo"                "viitevalialkuyksikko"
    #[25] "viitevaliloppuarvo"               "viitevaliloppuyksikko"
    
    
    
    #Below freetext checks
    
    #kanta <- fread("finngen_R14_kanta_laboratory_responses_freetext_raw_2016_internal_1.0.txt", data.table=F)
    
    #TUTKIMUSTULOSTEKSTI is the freetextvariable
    
    #####Change hetus to NNNNN
    hetu_regex <- "[0-9]{6}[-+aA][0-9]{3}[0-9a-zA-Z]"
    #This should find all cases, for example: 
    #"Potilas 123456-123A", "Potilas 123456-123a", "Potilas123456-123A", "123456-123APotilas", "Vuode123456-123APotilas"
    mahd_hetut <- regmatches(kanta_avo$tutkimustulosteksti, gregexpr(hetu_regex, kanta_avo$tutkimustulosteksti, perl = TRUE))
    length(mahd_hetut) 
    head(mahd_hetut)
    
    kanta_avo$tutkimustulosteksti <- gsub(hetu_regex,"NNNNN",kanta_avo$tutkimustulosteksti)
    ##OK!
    
    ####
    ####Change phone numbers to NNNNN
    #DF14 used:
    phone_regex <- "(?:\\+358[\\s-]?[0-9]{6,9}|0[\\s-]?(?:40|44|41|45|50|9)[\\s-]?[0-9]{5,8})"
    #DF15 will be used:
    phone_regex <- "(?<![0-9])(?:\\+358[\\s-]?[0-9]{6,9}|09[\\s-]?[0-9]{7,8}|0(?:40|44|41|45|50)[\\s-]?[0-9]{6,8})(?![0-9])"
    
    kanta_avo$tutkimustulosteksti <- gsub(phone_regex,"NNNNN",kanta_avo$tutkimustulosteksti, perl=TRUE)
    
    
    ######Change names to NNNNN 
    #all not possible to remove so only most common ones and nchar>3
    
    # Lue nimet listasta (nimi_regex)
    #Men names
    name_regex <- read_excel(opt$etunimitilasto, sheet=1)
    name_regex <- subset(name_regex, Lukumäärä > 199)
    name_regex <- subset(name_regex, nchar(Etunimi)>3)
    #Take out names that are some other words
    name_regex <- subset(name_regex, Etunimi!="Otto" & Etunimi!="Axel" & Etunimi!="Toimi" & Etunimi!="Arvo" & Etunimi!="Into" & Etunimi!="Levi")
    dim(name_regex) #
    name_regex <- name_regex$Etunimi
    name_regex <- trimws(name_regex)
    
    # Remove special characters from regex
    name_regex <- gsub("[.()\\+*?^$\\[\\]\\\\-]", "", name_regex)
    
    #Nex finds "independent" names
    name_regex <- paste0("(?<=\\s|\\.|^)(", 
                            paste(name_regex, collapse="|"), 
                            ")(?=\\s|\\.|$)(?![A-Za-z])")
    
    # Replace names with NNNNN
    kanta_avo$tutkimustulosteksti <- gsub(name_regex, "NNNNN", kanta_avo$tutkimustulosteksti, perl=TRUE, ignore.case=TRUE)
    
    #############
    
    #Women names
    name_regex <- read_excel(opt$etunimitilasto, sheet=4)
    dim(name_regex) 
    head(name_regex)
    name_regex <- subset(name_regex, Lukumäärä > 199)
    name_regex <- subset(name_regex, nchar(Etunimi)>3)
    #Take out names that are some other words
    name_regex <- subset(name_regex, Etunimi!="Aina" & Etunimi!="Ruusu" & Etunimi!="Aamu" & Etunimi!="Ilta" & Etunimi!="Anja" & Etunimi!="Gerd" & Etunimi!="Alla" & Etunimi!="Vera" & Etunimi!="Lisa" & Etunimi!="Anna" & Etunimi!="Erin" & Etunimi!="Ilma" & Etunimi!="Stella" & Etunimi!="Aura" & Etunimi!="Asta" & Etunimi!="Aava" & Etunimi!="Eden" & Etunimi!="Unna" & Etunimi!="Juni" & Etunimi!="Inga" & Etunimi!="Jana" & Etunimi!="Rosa" & Etunimi!="Salli" & Etunimi!="Laila" & Etunimi!="Maria" & Etunimi!="Marian" & Etunimi!="Säde" & Etunimi!="Yasmin" & Etunimi!="Ella")
    dim(name_regex)
    name_regex <- name_regex$Etunimi
    name_regex <- trimws(name_regex)
    
    # Remove special characters from regex
    name_regex <- gsub("[.()\\+*?^$\\[\\]\\\\-]", "", name_regex)
    
    #Nex finds "independent" names
    name_regex <- paste0("(?<=\\s|\\.|^)(", 
                            paste(name_regex, collapse="|"), 
                            ")(?=\\s|\\.|$)(?![A-Za-z])")
    
    # Replace names with NNNNN
    kanta_avo$tutkimustulosteksti <- gsub(name_regex, "NNNNN", kanta_avo$tutkimustulosteksti, perl=TRUE, ignore.case=TRUE)
    
    #############
    
    #Lastnames
    name_regex <- read_excel(opt$sukunimitilasto, sheet=1)
    dim(name_regex)
    head(name_regex)
    name_regex <- subset(name_regex, Yhteensä > 299)
    name_regex <- subset(name_regex, nchar(Sukunimi)>3)
    #Take out names that are some other words
    name_regex <- subset(name_regex, Sukunimi!="Back" & Sukunimi!="Korva" & Sukunimi!="Rask" & Sukunimi!="Stark" & Sukunimi!="Polvi" & Sukunimi!="Pesu" & Sukunimi!="Niska" & Sukunimi!="Joutsen" & Sukunimi!="Ranne" & Sukunimi!="Viik" & Sukunimi!="Lonka" & Sukunimi!="Sjögren" & Sukunimi!="Lund" & Sukunimi!="Riski" & Sukunimi!="Kivi" & Sukunimi!="Grön" & Sukunimi!="Valta" & Sukunimi!="Saari" & Sukunimi!="Vaara" & Sukunimi!="Sarja" & Sukunimi!="Lång" & Sukunimi!="Koivu" & Sukunimi!="Aura" & Sukunimi!="Lähde" & Sukunimi!="Kraft" & Sukunimi!="Koli" & Sukunimi!="Kela" & Sukunimi!="Kuha" & Sukunimi!="Valve" & Sukunimi!="Pohja" & Sukunimi!="Johnson" & Sukunimi!="Rinta" & Sukunimi!="From" & Sukunimi!="Vallin" & Sukunimi!="Kolari" & Sukunimi!="Selin" & Sukunimi!="Liuska" & Sukunimi!="Koho")
    dim(name_regex)
    name_regex <- name_regex$Sukunimi
    name_regex <- trimws(name_regex)
    
    # Remove special characters from regex
    name_regex <- gsub("[.()\\+*?^$\\[\\]\\\\-]", "", name_regex)
    
    #Nex finds "independent" names
    name_regex <- paste0("(?<=\\s|\\.|^)(", 
                            paste(name_regex, collapse="|"), 
                            ")(?=\\s|\\.|$)(?![A-Za-z])")
    
    # Replace names with NNNNN
    kanta_avo$tutkimustulosteksti <- gsub(name_regex, "NNNNN", kanta_avo$tutkimustulosteksti, perl=TRUE, ignore.case=TRUE)
    
    
    ####Next time also dates?
    
    #write out final kanta_freetext
    
    kanta_avo <- select(kanta_avo,"FINNGENID", "EVENT_AGE", 
                    "APPROX_EVENT_DAY", "TIME", "asiakirjaoid_pseudo", "merkintaoid_pseudo", "entryoid_pseudo", "load_id_pseudo", "file_name_pseudo",
                    "tutkimustulosteksti")
    
    dim(kanta_avo) 
    
    #write out
    fwrite(kanta_avo, file=opt$output_2,quote=T,sep="\t",na="NA", append=!first_chunk, col.names=first_chunk, nThread=1, compress="gzip")
    chunk_kept_rows_ <- nrow(kanta)
    n_total <<- n_total + chunk_kept_rows_
    n_chunk <<- n_chunk + 1L
    if (isTRUE(opt$debug)) {
      message(sprintf("  chunk %d | %d/%d (%.0f%%) | total kept: %d", n_chunk, chunk_kept_rows_, chunk_input_rows_, 100*chunk_kept_rows_/chunk_input_rows_, n_total))
    } else {
      message(sprintf("  chunk %d | %d/%d (%.0f%%)", n_chunk, chunk_kept_rows_, chunk_input_rows_, 100*chunk_kept_rows_/chunk_input_rows_))
    }
    first_chunk <<- FALSE
    invisible(NULL)  # prevent FALSE return from stopping readr iteration
  }),
  chunk_size = opt$chunk_size,
  col_types  = cols(.default = col_character()),
  progress   = FALSE
))
message("Done. Total rows processed: ", n_total)


gc()
