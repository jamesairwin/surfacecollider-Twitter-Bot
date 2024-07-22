import os

def check_file_creation_and_permissions():
    file_path = 'last_entry_id.txt'
    
    print("Current working directory:", os.getcwd())
    print("Directory contents:", os.listdir('.'))

    if os.path.exists(file_path):
        print(f"File '{file_path}' exists.")
        if os.access(file_path, os.W_OK):
            print(f"File '{file_path}' is writable.")
        else:
            print(f"File '{file_path}' is not writable.")
    else:
        print(f"File '{file_path}' does not exist. Creating file.")
        with open(file_path, 'w') as f:
            f.write("0")  # Initialize with a valid integer value.
        print(f"File '{file_path}' created with initial value '0'.")

if __name__ == "__main__":
    check_file_creation_and_permissions()
