import glob


def get_file_list(file_path):
    try:
        files = glob.glob(file_path + '//x*.gz')
    except:
        print('No files available yet')
    return files