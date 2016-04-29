This project explores chemicals used most commonly in consumer and industrial products, and attempts to capture their toxicity and exposure levels. The inspiration for the project is the website http://www.goodguide.com. This website uses over a 1,000 data sources to gather information about chemicals used in products and their impact on human health and the environment. 

The goal with this project is to gather information about chemicals, their toxicity to humans and the environment, and the products and categories they are used in, and to allow efficient querying of this data.

DATA SOURCES:

1. Chemical Product Categories database (http://actor.epa.gov/cpcat/faces/home.xhtml): This is a database of over 43,000 chemicals and over 1,000 categories of use, and over 200,000+ products that use these chemicals. Examples of categories are "manufacturing", "personal_care", "adhesive", etc. Categories may be composed of multiple words, with each word adding a new level of information akin to a hierarchy. To scope the project, I selected just the first word of the category and formed around 200 new "super categories".

- chemicals.txt: This file contains the chemical name and unique identifier (CASRN - Chemical Abstracts Service Registry Number). 43,000+ chemicals.
- cpcat_chemicals.txt: This file contains the categories that each chemical is found in. There is a row per unique combination of chemical and category. As mentioned before, the categories were pre-processed to extract the first word as the category name. 500,000+ entries.
- products.txt: This file contains product names and the chemicals found in the product. There is a row per unique combination of product and chemical. 800,000+ entries.


2.  Safer chemicals: (https://www.epa.gov/saferchoice/safer-ingredients) : contains a list of safe chemicals

3. Toxic chemicals:
    Recognized cancer causing
    http://scorecard.goodguide.com/health-effects/chemicals.tcl?short_hazard_name=cancer&all_p=t
    Recognized developmental toxicity
    http://scorecard.goodguide.com/health-effects/chemicals.tcl?short_hazard_name=devel&all_p=t
    Recognized reproductive toxicity
    http://scorecard.goodguide.com/health-effects/chemicals.tcl?short_hazard_name=repro&all_p=t
    Bioaccumulative Chemicals of Concern (U.S. Environmental Protection Agency)
    http://scorecard.goodguide.com/chemical-groups/one-list.tcl?short_list_name=bcc
    Environment: Dangerous for the Environment (Nordic Council of Ministers)
    http://scorecard.goodguide.com/chemical-groups/one-list.tcl?short_list_name=dfe
    Ozone Depleting Substances (Montreal Protocol)
    http://scorecard.goodguide.com/chemical-groups/one-list.tcl?short_list_name=ods
    Persistent, Bioaccumulative, and Toxic Chemicals (U.S. Environmental Protection Agency)
    Greenhouse gases
    http://scorecard.goodguide.com/chemical-groups/one-list.tcl?short_list_name=gg


A note about the data sources: There is a large amount of information available publicly about chemicals and toxicity. CpCat database is an attempt by the EPA to create a comprehensive "exposure profile" for each known chemical. For toxic chemicals, there are several data sources, and not a single comprehensive one. I eventually chose a subset of the chemicals reported in the GoodGuide's scorecard website. The list of toxic and safe chemicals together do not cover all of the 43,000+ chemicals in CPCat: this is partly due to the selective toxicity data sources used in the project, but also more due the fact that very few chemicals have been tested and quantified for toxicity. The CPCat database is a starting point for researchers to determine which chemicals have high exposure profiles, and focus their limited budgets and efforts on testing those chemicals first.


FOLDER AND FILE STRUCTURE

1. Data: this folder contains all the data used in the project, as mentioned in the section "Data Sources". The files are required to be present on the EC2 instance at the path: /home/w205/project/data/WithHeaders

2. ELT: This folder contains the scripts required for the Extract, Load and Transform step. There is a single python script in this location called cpact_ELT.py.
    - Make sure the source files are in the location specified in step1.
    - Create a folder in HDFS to hold the parquet files that are written out: hdfs dfs -mkdir /user/w205/project
    - Run the script using the command: spark-submit --packages com.databricks:spark-csv_2.10:1.2.0 cpcat_ETL.py

The ELT script uses pyspark DataFrames to process the source files. It creates 3 output parquet files:
    - dfchemicals.parquet: Contains the chemical name and information on the chemical - the number of categories it appears in, the number of products it appears in, whether it is safe or toxic (with 8 toxicity categories); and an overall toxicity score based on how many toxicity categories the chemical appears in.
    
    - dfcategories.parquet: Contains category names and summary information for each category: total number of checmicals, number of safe chemicals, number of toxic chemicals, etc.

    - dfproducts.parquet: Contains product name and chemical name, not changed much from the original products file.

3. Queries: This folder contains python scripts that can be invoked to query the database. The following scripts are provided:

    - chem.py [-n name | -c CASRN] [-t toxic|safe]: returns a list of chemicals that match the specified criteria
        - use -n to search by name (partial names accepted, no regular expressionss) or -c to search by CASRN
        - -t toxic returns the list sorted by most to least toxic (default); -t safe sorts from least to most toxic
        
    - chemcat.py -n name [-t toxic | safe]: returns a list of categories that match the specified criteria
        - use -n to search by name (partial names accepted, no regular expressionss) 
        - -t toxic returns the list sorted by most to least toxic (default); -t safe sorts from least to most toxic
    
    -products.py -n name [-t toxic | safe] : returns a list of products that match the specified criteria.
        - use -n to search by name (partial names accepted, no regular expressionss) 
        - -t toxic returns the product list sorted by the number of toxic chemicals found in the product; -t safe sorts the list in reverse order
    
    - chemlist.py -n N -t toxic|safe : returns a list of the top N chemicals ordered by their toxicity score, descending if toxic=1 and ascending otherwise.
    
    - chemcatlist.py -n N -t toxic|safe : returns a list of the top N chemical categories ordered by their toxicity score, descending if toxic=1 and ascending otherwise.
    
    - productlist.py -n N -t toxic|safe: returns a list of the top N products ordered by the number of toxic chemicals found in them. Sort order is descending if toxic=1 and ascending otherwise.
    




    
    
