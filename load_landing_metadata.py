import utilis
from load_dataframe_to_sf import *
from load_yaml_file import *
from utilis.list_files import *

### split the large files into chunk files based on the lines
#### list the all the files related review files load

# Get the list of files
def get_file_list(file_path):
    files = glob.glob(file_path + '//x*.gz')
    return files

def execute_load():
    # Load the yaml arguments and config
    ld_yml = load_yaml_file('load_landing_metadata.yaml')

    file_path = ld_yml['snowflake']['file_path']
    table_nam = ld_yml['snowflake']['table_name']
    source = ld_yml['snowflake']['source']
    layer = ld_yml['snowflake']['layer']

    # List the files to be processed
    file_list = get_file_list(file_path)

    # Process the files -- Dataframe to Snowflake table load
    for file_name in file_list:
        file_name = file_name.replace('\\', '//')
        # print(file_name + 'loaded')
        try:
            utilis.audit_file_load.insert_file_load(source, table_nam, layer, 'start', 0, file_name)
            load = data_load(file_name, table_nam)
            success, nrows = load.df_load()
            if success:
                utilis.audit_file_load.insert_file_load(source, table_nam, layer, 'Success', nrows, file_name)
                # print('yes')
            else:
                utilis.audit_file_load.insert_file_load(source, table_nam, layer, 'Failed', 0, file_name)

        except:

            print(file_name + 'not loaded')
            continue
    return 0


