********************************************************************************
/* 
Use the queried PIP distribution and add the missing countries to create a 
	global 1000 binned distribution.
*/
********************************************************************************

**********************
*** 01. INPUT DATA ***
**********************

*** PIP Options ***
pip cleanup
global options = "ppp_year(2017) clear"


*** PIP regions ***
pip tables, ${options} table(country_coverage)
keep country_code pcn_region_code
ren pcn_region region_code
duplicates drop
tempfile regions
save `regions'


*** pip population ***
pip tables, ${options} table(pop) 
foreach v of varlist v3-v46 {
   local x : variable label `v'
   rename `v' year_`x'
}
reshape long year_ , i(country_code data_level) j(year)
ren year_ pop
ren data_level reporting_level
keep if reporting_level=="national" | inlist(country_code, "ARG", "SUR")
keep if year>1980 & year<2020
replace pop = pop/1000000 												// pop in millions 
merge m:1 country_code using `regions', nogen
ren country_code code
isid code reporting_level year 
tempfile pop_all
save `pop_all'


*********************************
*** 02. ADD MISSING COUNTRIES ***
*********************************

use "CollapsedDistributions.dta", clear
drop if missing(welf) 					

gen reporting_level="national"
replace reporting_level="urban" if inlist(code,"ARG","SUR")		// Argentina and Suriname only have urban surveys, rural gets regional average

merge m:1 code reporting_level year using `pop_all', nogen 		// population and region
keep if inrange(year,1990,2019)
drop if reporting_level=="national" & inlist(code, "ARG", "SUR")

sum obs
expand `r(max)' if missing(welf)
bys year code reporting_level (obs): replace obs = _n if missing(obs)
bys region_code year obs: egen avg_welf=wtmean(welf), weight(pop)
replace welf = avg_welf if missing(welf)
drop avg_welf

replace pop = pop/1000											// change to bin population from country pop
cap gen _mi_miss=0 												// remove multiple imputation
cap mi extract 0, clear
cap drop _mi_miss 

compress
order year code reporting_level region_code obs welf
sort year code reporting_level obs
label var obs 			  "Bin number"
label var reporting_level "Regional coverage of the country"
label var welf 			  "Average daily welfare of bin in 2017 PPP USD"
label var pop 			  "Population of bin in millions"

save "GlobalDist1000bins_1990-2019_RuralUrban.dta", replace		// ARG, SUR have Rural/Urban; all else national only


*******************************************************************
*** 03. KEEP ONLY NATIONAL DISTRIBUTION; 218 NATIONAL 1000 BINS ***
*******************************************************************

*** Start with empty 1000 bins for ARG and SUR to be filled in ***

clear all
set obs 2
gen 	code = "ARG"
replace code = "SUR" in 2
expand 30										// number of years 1990-2019
gen year = 1990
bys code: replace year = year[_n-1]+1 if _n!=1
expand 1000 									// 1000 observations per year
bys code year: gen obs = _n
tempfile dist
save 	`dist'


*** Collapse to 1000 national bins for ARG and SUR ***

use "${path}02-rawdata\GlobalDist1000bins_1990-2019_RuralUrban.dta", clear
keep if inlist(code,"ARG","SUR")

preserve
keep code region_code
duplicates drop
tempfile region_var
save 	`region_var'
restore

egen id = group(code year)

egen bins = xtile(welf), by(id) n(1000) weight(pop)	
collapse welf (rawsum) pop [aw=pop], by(bins code year)
ren bins obs

merge 1:1 code year obs using `dist'

*** replace welfare and population for missing bins (xtile combines all bins that have repeated observation, so those bins) ***
bys code year (obs): 	 gen 	 tag = obs 			if (_merge!=_merge[_n+1]) & _merge[_n+1]==2 		// tagging repeated bins
bys code year (obs): 	 replace tag = tag[_n-1] 	if _merge==2
bys code year tag (obs): gen 	 num = _N 			if !missing(tag)									// number of repeated bins for adjust population

bys code year (obs): replace welf = welf[_n-1]  if missing(welf)				// welfare values should be same across those bins
bys code year (obs): replace pop  = pop[_n-1] 	if missing(pop)
bys code year (obs): replace pop  = pop/num 	if !missing(num)				// adjust the population across those bins

keep year code obs welf pop
merge m:1 code using `region_var', nogen

tempfile adjdist
save 	`adjdist'


*** Append the global distribution with collapsed national 1000 bins for ARG and SUR ***

use "${path}02-rawdata\GlobalDist1000bins_1990-2019_RuralUrban.dta", clear
drop if inlist(code,"ARG","SUR")												// drop rural/urban structure
drop reporting_level

append using `adjdist'

compress
order year code region_code obs welf
sort year code obs
label var obs 	"Bin number"
label var welf 	"Average daily welfare of bin in 2017 PPP USD"
label var pop 	"Population of bin in millions"

save "GlobalDist1000bins_1990-2019.dta", replace

********************************************************************************
exit
