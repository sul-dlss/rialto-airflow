---

# README.txt

This README.txt file was generated in July 2026.

--------------------  
GENERAL INFORMATION  
--------------------

1. Title of Dataset:  
Open dataset of annual Article Processing Charges (APCs) of gold and hybrid journals published by 14 scholarly publishers, 2019–2025

2. Author Information

A. Name: Lisa Matthias (Corresponding Author)  
   Institution: Humboldt-Universität zu Berlin / Scholarly Communications Lab  
   Email: l.a.matthia@gmail.com

B. Name: Diego Chavarro  
   Institution: Simon Fraser University / University of Ottawa  
   Email: dchavarro@gmail.com

C. Name: Eric Schares  
   Institution: Iowa State University / Scholarly Communications Lab    
   Email: eschares@iastate.edu

D. Name: Juan Pablo Alperin  
   Institution: Simon Fraser University / Scholarly Communications Lab 
   Email: juan@alperin.ca

E. Name: Margaret Rose  
   Institution: University of Ottawa / Scholarly Communications Lab    
   Email: mrose036@uottawa.ca

F. Name: Molly Frost  
   Institution: University of Ottawa / Scholarly Communications Lab 
   Email: mfros090@uottawa.ca

G. Name: Flavia Camargo  
   Institution: University of Ottawa / Scholarly Communications Lab   
   Email: fdeso073@uottawa.ca

H. Name: Jonas Höfting  
   Institution: Humboldt-Universität zu Berlin / Scholarly Communications Lab  
   Email: jonas.hoefting@hu-berlin.de

I. Name: Leigh-Ann Butler  
   Institution: University of Ottawa / Scholarly Communications Lab   
   Email: leigh-ann.butler@uottawa.ca

J. Name: Nina Schönfelder  
   Institution: University Library,  Bielefeld University  
   Email: nina.schoenfelder@uni-bielefeld.de

K. Name: Stefanie Haustein
   Institution: University of Ottawa / Université du Québec à Montréal (CIRST) / 
   Scholarly Communications Lab    
   Email: stefanie.haustein@uottawa.ca

---

3. Collection instrument:  
Manual data collection, web scraping, and algorithmic parsing.

4. How to cite:  
Matthias, L., Chavarro, D., Schares, E., Alperin, J.P., Rose, M., Frost, M., Camargo, F., Höfting, J., Butler, L.-A., Schönfelder, N., & Haustein, S. (2026). *Open dataset of annual Article Processing Charges (APCs) of gold and hybrid journals published by 14 scholarly publishers, 2019–2025* [Dataset]. Harvard Dataverse.

---------------------------  
SHARING/ACCESS INFORMATION  
---------------------------

Licenses/restrictions placed on the data:  
This dataset is released under a CC0 Public Domain Dedication.  
`https://creativecommons.org/public-domain/cc0/` [(creativecommons.org)]

---------------------  
DATA & FILE OVERVIEW  
---------------------


1. File List

A. scholcommlab_apc_dataset_2019_2025_data_dictionary.pdf  
   Description: Codebook describing all variables.

B. scholcommlab_apc_dataset_2019_2025.csv  
   Description: Annual APC dataset.

C. scholcommlab_apc_dataset_2019_2025_conversion_rates.txt  
   Description: Annual average currency conversion rates used.


2. Additional related data not included in this package:  
Original publisher APC price lists and archived Wayback Machine snapshots.

---------------------------  
METHODOLOGICAL INFORMATION  
---------------------------

1. Description of methods used for collection/generation of data:

This dataset combines and standardizes APC list prices from 14 major scholarly publishers—ACS, Cambridge University Press, De Gruyter, EDP Sciences, Elsevier, Frontiers, IEEE, IOP, MDPI, Oxford University Press, PLOS, Sage, Springer Nature, and Wiley—covering the years 2019–2025.

Price lists were retrieved from publisher websites and archived snapshots in the Wayback Machine. APCs were extracted from HTML pages, PDFs, and XLSX files; when no downloadable list existed, APCs were manually collected or scraped from individual journal pages.

The dataset includes APC list prices, OA status, journal metadata, collection dates, and APC values in multiple currencies (USD, EUR, GBP, CHF, JPY, CAD, AUD). It contains 12,540 unique journals and 69,856 journal-year combinations.

2. Methods for processing the data:

Data were processed through a combination of manual entry and automated parsing scripts.  
Key steps included:

- Standardizing journal titles, publisher names, and ISSNs.  
- Validating ISSNs using the ISSN.org portal and fuzzy matching.  
- Converting APCs into seven currencies using annual average exchange rates.  
- Merging all publisher lists into a unified schema.  
- Removing duplicates and resolving publisher transfers.  
- Recording flat-rate APCs numerically and complex fee structures in text form.  
- Adding internal record-level and journal-level identifiers.

3. Instrument- or software-specific information needed to interpret the data:  
The dataset is provided as tab-delimited text files.  
The Codebook (scholcommlab_apc_dataset_2019_2025_data_dictionary.pdf) is required to interpret variable names and formats.

------------------------------------------------------------------  
DATA-SPECIFIC INFORMATION FOR: scholcommlab_apc_dataset_2019_2025  
------------------------------------------------------------------

1. Number of variables: 30  
2. Number of cases/rows: 69,856  

Refer to scholcommlab_apc_dataset_2019_2025_data_dictionary.pdf for detailed variable information.

---
