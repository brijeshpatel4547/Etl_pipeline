# Extract data from the CSV, JSON and XML file
# xml library used to parse .xml file
# csv and json files are read by pandas library hence importing libraries

import pandas as pd
import glob
import xml.etree.ElementTree as et
from datetime import datetime

# Adding File paths: 1. For transformed data, 2. For Log files
log_file = "log_file.txt"
target_file = "transformed.csv"

"""------------------------Extraction from the files - Extract(E)-----------------------------"""


# Extract from csv function
def extract_csv(file):
    csv_df = pd.read_csv(file)
    return csv_df


# Extract from Json
def extract_json(file):
    json_df = pd.read_json(file,lines= True)
    return json_df


# Extract from XML
def extract_xml(file):
    columns = ["name", "height", "weight"]
    xml_df = pd.DataFrame(columns=columns)
    # Parse the file using parse function
    tree = et.parse(file)
    root = tree.getroot()

    for record in root:
        name = record.find("name").text
        height = float(record.find("height").text)
        weight = float(record.find("weight").text)

        xml_df = pd.concat([xml_df, pd.DataFrame([{"name": name, "height": height, "weight": weight}])],
                           ignore_index=True)

    return xml_df


def extract():
    """ this function will be used to call the extract file functions
    based on the type of file encountered"""
    extracted_df = pd.DataFrame(columns=["name", "height", "weight"])

    # Extract from all CSV files

    for csv_file in glob.glob("Data/*.csv"):
        extracted_df = pd.concat([extracted_df, pd.DataFrame(extract_csv(csv_file))], ignore_index=True)

    # Extract from all JSON files
    for json_file in glob.glob("Data/*.json"):
        extracted_df = pd.concat([extracted_df, pd.DataFrame(extract_json(json_file))], ignore_index=True)

    # Extract from all XML files
    for xml_file in glob.glob("Data/*.xml"):
        extracted_df = pd.concat([extracted_df, pd.DataFrame(extract_xml(xml_file))], ignore_index=True)

    return extracted_df


"""----------------------------- Transformations --------------------------------"""

""" The data extracted above has the height in inches and weight in pounds hence we need to convert them into meters and
    Kilograms respectively"""


def transform(data):
    """ Inches to Meters and round to 2 decimals
        1 inch = 0.0254 Mts"""

    data['height'] = round(data.height * 0.0254, 2)

    """ Pounds to Kilograms and round to 2 decimals
        1 Pound = 0.45359237 Kg """
    data['weight'] = round(data.weight * 0.45359237, 2)

    return data


"""----------------------------- Loading and Logging the data --------------------------------"""


def load(target_file, transformed_data):
    transformed_data.to_csv(target_file)


def logging(msg):
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    # Get current timestamp
    now = datetime.now()
    # using the strftime, we will format the returned time to our required format
    timestamp = now.strftime(timestamp_format)

    with open(log_file, "a") as file:
        file.write(timestamp + " : \t" + msg + "\n")


# Log the initialization of the ETL process
logging("ETL Job Started")

# Log the beginning of the Extraction process
logging("Extract phase Started")
extracted_data = extract()

# Log the completion of the Extraction process
logging("Extract phase Ended")

# Log the beginning of the Transformation process
logging("Transform phase Started")
transformed_data = transform(extracted_data)
print("Transformed Data")
print(transformed_data)

# Log the completion of the Transformation process
logging("Transform phase Ended")

# Log the beginning of the Loading process
logging("Load phase Started")
load(target_file, transformed_data)

# Log the completion of the Loading process
logging("Load phase Ended")

# Log the completion of the ETL process
logging("ETL Job Ended")
