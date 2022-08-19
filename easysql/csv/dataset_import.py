import pandas
import os
import math

def format_token(token, to_type: str):
    """Converts an input token to oracle sql supported
       syntax using to_type
       :param token: input token 
       :param to_type: sql target query to support the to_type format
    """
    try:
        if(type(token) != str and math.isnan(float(token))):
            raise ValueError
    except ValueError:
        return 'NULL'

    if(to_type.lower() == 'int'):
        return str(int(token))
    elif(to_type.lower() == 'decimal'):
        return str(float(token))
    elif(to_type.lower() == 'varchar2'):
        # replaces single quotes within a str with two single quotes
        token = str(token).replace('\'','\'\'')
        return str('\''+token+'\'')
    elif('timestamp_' in to_type.lower()):
        return 'TO_TIMESTAMP(\'' + str(token) + '\',\'' + ''.join(to_type.lower().split('timestamp_')) + '\')'
    elif('date_' in to_type.lower()):
        return 'TO_DATE(\'' + str(token) + '\',\'' + ''.join(to_type.lower().split('date_')) + '\')'
    else:
        return str(token)

def csv_to_sql(csvfile: str, txtfile: str, tablename: str, tableinfo: list):
    """Writes to a txt file with the oracle sql insert commands
       after reading from csv file.
       :param csvfile: Path to the csv dataset
       :param txtfile: Path to the output txt file
       :param tableinfo: list containing datatype of table columns
    """
    
    query_p1 = 'INSERT INTO ' + tablename + '('
    query_p2 = ') VALUES('
    query_p3 = ');'

    df = pandas.read_csv(csvfile)
    #print(df)

    tablecolumns = ','.join(list(df.columns))

    processed_rows = []

    for i in range(len(df)):
        processed_values_string = ''
        j = 0
        for col in df.columns:
            processed_values_string += (',' if(j!=0) else '') + format_token(df.loc[i,col],tableinfo[j])
            j+=1   
        processed_rows.append(query_p1 + tablecolumns + query_p2 + processed_values_string + query_p3)
    
    with open(txtfile, 'w') as fp_w:
        fp_w.write('\n'.join(processed_rows))