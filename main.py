from table_creation import main as create_table_main
from preprocessing import main as etl_main
#Process starts here
if __name__ == "__main__":
    create_table_main()
    etl_main()
