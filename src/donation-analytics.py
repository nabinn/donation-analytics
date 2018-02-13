import sys
import time
import math
import bisect
from datetime import datetime
from collections import defaultdict

# data fields as given here: https://goo.gl/89YTjW
FIELDS = "CMTE_ID,AMNDT_IND,RPT_TP,TRANSACTION_PGI,IMAGE_NUM,\
        TRANSACTION_TP,ENTITY_TP,NAME,CITY,STATE,ZIP_CODE,EMPLOYER,\
        OCCUPATION,TRANSACTION_DT,TRANSACTION_AMT,OTHER_ID,TRAN_ID,\
        FILE_NUM,MEMO_CD,MEMO_TEXT,SUB_ID"
FIELDS_LIST = FIELDS.replace('\n', '').replace('\t', '').split(",")

# Map the string fields to the corresponding index values
field_to_index = {k: v for v, k in enumerate(FIELDS_LIST)}


def print_donations(mydata):
    for k1, v1 in mydata.items():
        print(f"{k1}:")
        for k2, v2 in v1.items():
            print(f"\t{k2}:")
            for k3, v3 in v2.items():
                print(f"\t\t{k3}:")
                for k4, v4 in v3.items():
                    print(f"\t\t\t{k4}=>{v4}")


def get_percentile_index(n, p):
    """Referenced from https://en.wikipedia.org/wiki/Percentile
    :param n: size of sorted array
    :param p: percentile value
    :return: index of pth percentile in the array
    """
    ordinal_rank = math.ceil(p * n / 100)
    # since indexing starts from zero
    return ordinal_rank - 1


def is_valid_date(date_str):
    """
    :param date_str: date in string format
    :return: boolean indicating valid date
    """
    valid_date = None
    try:
        datetime.strptime(date_str, '%m%d%Y')
        valid_date = True
    except ValueError:
        valid_date = False

    return valid_date


def get_year(date_str):
    """
    :param date_str: date in string format
    :return: value of year
    """
    date_obj = datetime.strptime(date_str, '%m%d%Y')
    return date_obj.year


def validate_record(record):
    """
    :param record: string containing one record from the data file
    :return: a tuple containing (is_valid, data);
    is_valid is a flag indicating whether data is valid or not
    and data is a tuple with required fields
    """
    is_valid = True
    data = None

    record = record.split("|")
    # if after splitting, the record does not contains same number
    # of items as FIELD_LIST, then it cannot be a valid record
    if len(record) < len(FIELDS_LIST):
        is_valid = False
    else:
        cmte_id = record[field_to_index["CMTE_ID"]]
        other_id = record[field_to_index["OTHER_ID"]]
        name = record[field_to_index["NAME"]]
        zip_code = record[field_to_index["ZIP_CODE"]]
        transaction_dt = record[field_to_index["TRANSACTION_DT"]]
        transaction_amt = record[field_to_index["TRANSACTION_AMT"]]

        # The record is invalid in following conditions:
        # if CMTE_ID is empty
        # if the OTHER_ID field is not empty
        # if the NAME is an invalid name (e.g., empty, malformed)
        # if ZIP_CODE is an invalid zip code (i.e., empty, fewer than five digits)
        # if TRANSACTION_DT is an invalid date
        # if any lines in the input file contains empty cells in TRANSACTION_AMT fields
        if (cmte_id == "" or
                other_id != "" or
                name == "" or
                zip_code == "" or len(zip_code) < 5 or
                not is_valid_date(transaction_dt) or
                transaction_amt == ""):
            is_valid = False

        else:
            # converting donation amount to whole dollar value
            transaction_amt = math.ceil(float(transaction_amt))
            # ignore negative or zero amounts
            if transaction_amt <= 0:
                is_valid = False
            else:
                data = (cmte_id, name, zip_code[:5], get_year(transaction_dt), transaction_amt)

    return is_valid, data


def nested_dict():
    return defaultdict(nested_dict)


def read_file_and_process_data(input_data_file, output_file, percentile_value):
    """ reads data from input file line by line and writes result to output file based on percentile value
        :param input_data_file: text file containing pipe delimited data
        :param output_file: text file to write result
        :param percentile_value: the percentile value to use for processing data
        :return: None
    """
    donors = {}  # dictionary to store donor information
    donations = nested_dict()  # nested dictionary to store donation information
    try:
        output_file = open(output_file, "w")
        with open(input_data_file, 'r') as f:
            num_lines = 0
            num_valid_records = 0

            # process the file line by line as if it were streaming data
            for line in f:
                # check if the line contains valid record
                valid_record, record = validate_record(line)

                # add valid record to donations
                if valid_record:
                    num_valid_records += 1
                    cmte_id, name, zip_code, year, transaction_amt = record
                    try:
                        # keep track of number of donations
                        donations[cmte_id][zip_code][year]["count"] += 1
                        # keep track of total donations
                        donations[cmte_id][zip_code][year]["sum"] += transaction_amt
                        # insert the amount to the list such that the list remains sorted
                        bisect.insort(donations[cmte_id][zip_code][year]["donations"], transaction_amt)

                    except (TypeError, KeyError):
                        donations[cmte_id][zip_code][year]["count"] = 1
                        donations[cmte_id][zip_code][year]["sum"] = transaction_amt
                        donations[cmte_id][zip_code][year]["donations"] = [transaction_amt]

                    # if this donor is a repeat donor, write data to output file
                    if (name, zip_code) in donors and year > donors[(name, zip_code)]:
                        num_donations = donations[cmte_id][zip_code][year]["count"]
                        total_amt = donations[cmte_id][zip_code][year]["sum"]
                        percentile_index = get_percentile_index(num_donations, percentile_value)
                        percentile_amt = donations[cmte_id][zip_code][year]["donations"][percentile_index]
                        output_file.write(f"{cmte_id}|{zip_code}|{year}|{percentile_amt}|{total_amt}|{num_donations}\n")
                    else:
                        # add this donor to donors
                        donors[(name, zip_code)] = year

                num_lines += 1

            print(f"Total lines processed: {num_lines}")
            print(f"Total valid records: {num_valid_records}")
        #print_donations(donations)

    except Exception as ex:
        print(f"Following exception occured:\n{ex}")
    finally:
        output_file.close()


def read_percentile_from_file(filename):
    """ reads data from file and returns it
    """
    try:
        with open(filename, 'r') as f:
            percentile_value = int(f.read().strip())
    except (IOError, TypeError):
        print("Couldn't read percentile value")
        percentile_value = -1
    return percentile_value


if __name__ == "__main__":
    start = time.time()
    try:
        input_data_file = sys.argv[1]  # main data file
        input_percentile_file = sys.argv[2]  # text file containing percentile value
        output_file = sys.argv[3]  # output File to store result
    except IndexError:
        print("Usage: ")
        print("python ./src/donation-analytics.py ./input/itcont.txt ./input/percentile.txt ./output/repeat_donors.txt")
        sys.exit(1)

    percentile_value = read_percentile_from_file(input_percentile_file)
    if percentile_value < 0 or percentile_value > 100:
        print("percentile value should be between 0 and 100")
        sys.exit(1)

    read_file_and_process_data(input_data_file, output_file, percentile_value)

    end = time.time()
    print("Total elapsed time : {:.3f} seconds".format(end - start))
