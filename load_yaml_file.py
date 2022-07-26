import yaml
import os
def load_yaml_file(file_name):
    dir_path  = "C://Users//apeks//PycharmProjects//amazon_product_data_analysis//"                      #----os.getcwd()
    file_path = os.path.join(dir_path, "config", file_name)
    file_path = file_path.replace("\\","//")
    try:

        with open(file_path, 'r') as file:
            prime_service = yaml.safe_load(file)
        return prime_service
    except yaml.YAMLError:
        print("Error in configuration file:")

