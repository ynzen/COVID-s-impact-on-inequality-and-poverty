********************************************************************************
/*
Query poverty lines in PIP to create 1000 binned lineup distribution for each country/year.
Output data structure is balanced panel with 169 countries each with 1000 observations.  	
*/
********************************************************************************
clear all
set more off
pip cleanup

************************
*** 01. QUERYING PIP ***
************************

*** generate poverty lines to query ***

clear
set obs 100000
gen double povertyline = _n/200
replace povertyline = povertyline[_n-1]*1.0025 if povertyline>50 			// Chose increments of 0.005 until 50, and then increase of 0.25% 
drop if povertyline>900
replace povertyline = round(povertyline,0.001)
tostring povertyline, replace force
gen povertyline5 = povertyline + " " + povertyline[_n+1] + " " + povertyline[_n+2] + " " + povertyline[_n+3] + " " + povertyline[_n+4]
keep if mod(_n-1,5)==0
drop povertyline


*** query PIP ***

qui levelsof povertyline5
foreach lvl in `r(levels)' {
	disp as error "`lvl'"
	qui pip, country(all) year(all) povline(`lvl') ppp_year(2017) fillgaps clear 
	keep if inlist(reporting_level,"national") | inlist(country_code,"ARG","SUR") 	// keep national estimates
	ren country_code countrycode
	ren poverty_line povertyline
	keep countrycode reporting_level year povertyline headcount poverty_gap poverty_severity
	cap append using `datasofar'
	tempfile datasofar
	qui save `datasofar' 
	qui save  "FullDistributions.dta", replace 
}
	
*********************************************
*** 02. TURNING HEADCOUNT RATES INTO PDFs ***
*********************************************

use "FullDistributions.dta", clear

sort year countrycode povertyline
keep if inrange(year,1990,2019)													// keep 1990 - 2019 data


*** Generate weight ***
bys  year countrycode (povertyline): gen 	 weight = headcount - headcount[_n-1]
bys  year countrycode (povertyline): replace weight = headcount if _n==1
drop if weight <= 0														


*** Calcualte the cumulative average income up to and including a particular poverty threshold ***
bys  year countrycode (povertyline): gen avgincome_cumulative = povertyline * (1 - poverty_gap/headcount)


*** Calculate the average income of each bin ***
bys  year countrycode (povertyline): gen 	welfare = ((headcount*avgincome_cumulative) - (headcount[_n-1]*avgincome_cumulative[_n-1])) / (headcount - headcount[_n-1])
bys  year countrycode (povertyline): replace welfare = avgincome_cumulative if _n==1


*** Collapse to 1000 observation per country ***

forval yr = 1990/2019 {
	preserve
	keep if year==`yr'
	levelsof countrycode
	foreach cd in `r(levels)' {
		disp in red "`cd' `yr'"
		pctile welf`cd'=welfare [aw=weight] if countrycode=="`cd'" & year==`yr', nq(1001) gen(obs) 
		qui sum welfare [aw=weight] if countrycode=="`cd'"  & year==`yr', d		// change the welfare of top bin to adjust the distributional statistic
		pctile welftemp`cd'=welfare [aw=weight] if countrycode=="`cd'" & year==`yr' & welfare>`r(p90)', nq(1001) gen(obs_temp)
		qui replace welf`cd'=welftemp`cd'[1000] if _n==1000
		drop obs obs_temp welftemp*
	}
	keep if _n<=1000
	keep year welf*
	drop welfare
	gen obs = _n
	cap append using `combdata'
	tempfile 		 combdata
	save 			`combdata'
	restore
}

use `combdata'	, clear
reshape long welf, i(obs year) j(code) string
sort year code obs

lab var code 	"Country code"
lab var welf 	"Average daily welfare of bin in 2017 PPP USD"

save "CollapsedDistributions.dta", replace 

********************************************************************************
exit


