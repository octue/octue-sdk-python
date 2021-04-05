import re
import subprocess


def get_service_information(name):
    raw_output = subprocess.getoutput("gcloud run services list --platform managed")
    columns, data = raw_output.split("\n")
    columns = re.split(r"\s{2,}", columns.strip())
    number_of_columns = len(columns)
    data = re.split(r"\s{2,}", data.strip())[1:]

    if name not in data:
        return None

    name_index = data.index(name)
    relevant_data = data[name_index : name_index + number_of_columns]
    return {columns[i]: relevant_data[i] for i in range(number_of_columns)}
