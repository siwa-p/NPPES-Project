# NPPES Project

You work for a healthcare analytics company interested in identifying the healthcare providers located in each US county.

### Part 1
For this part, you've been provided the first 1000 rows from the April 2025 NPPES Data Dissemination File in the file nppes_sample.csv.

Your goal is to create a csv file containing the following information:
* 'NPI' 
* Entity Type, indicated by the 'Entity Type Code' field:
    - 1 = Provider (doctors, nurses, etc.)
    - 2 = Facility (Hospitals, Urgent Care, Doctors Offices) 
* Entity Name: Either First/Last or Organization or Other Organization Name contained in the following fields:
    - 'Provider Organization Name (Legal Business Name)'
    - 'Provider Last Name (Legal Name)'
    - 'Provider First Name'
    - 'Provider Middle Name'
    - 'Provider Name Prefix Text'
    - 'Provider Name Suffix Text'
    - 'Provider Credential Text'
* Practice Address: Use the Business Practice Location (not the mailing address) found in:
    - 'Provider First Line Business Practice Location Address'
    - 'Provider Second Line Business Practice Location Address'
    - 'Provider Business Practice Location Address City Name'
    - 'Provider Business Practice Location Address State Name'
    - 'Provider Business Practice Location Address Postal Code'
* The provider's primary taxonomy code, which is contained in one of the 'Healthcare Provider Taxonomy Code*' columns. A provider can have up to 15 taxonomy codes, but we want the one which has Primary Switch = Y in the associated 'Healthcare Provider Primary Taxonomy Switch*' field. Note that this does not always occur in spot 1.
* Grouping, Classification, and Specialization for the primary taxonomy code. This can be found in the NUCC Taxonomy file (nucc_taxonomy_250.csv).

### Part 2

Create a stored procedure to process rows from the NPPES data. Use this stored procedure in order to process the full NPPES data file (npidata_pfile_20050523-20250413.csv) and store the data in a database.

### Part 3

The company would like to analyze the number and type of healthcare provider in each county. Update your database to include information that will help determine the county of each provider.

To associate each provider with a county, you've been given:
- A ZIP code to county crosswalk file (ZIP_COUNTY_032025.xlsx).
- A FIPS code to county name crosswalk (ssa_fips_state_county_2025.csv)

In addition, you can pull population information through the US Census API at this URL: https://api.census.gov/data/2023/acs/acs5?get=NAME,B01001_001E&for=county:*

**Note 1:** The zip codes in the NPPEs file are not in a standard format, so you may need to do some cleaning to standardize them.

**Note 2:** Some ZIP codes cross county boundaries. In those cases, assign the county with the larger population.

### Stretch Goals
* NPPES releases weekly update files, which can be downloaded from https://download.cms.gov/nppes/NPI_Files.html. Update your database using the two most recent update files. Can you incorporate all updates that have occurred since the April Data Dissemination file? 
* NPPES also has a list of deactivated NPIs. Incorporate these deactivated NPIs into your database. Note that it would be preferable to keep all records rather than deleting records that have been deactivated.
* Additional variables could be helpful for the downstream analysis. Add in some additional information from the census, like the median income or median home value. 
* The ZIP to county crosswalk file includes ratio columns that show how much of each zipcode is included in a given county. If these are used instead of using the county population, are there any zipcodes that are assigned differently? 
