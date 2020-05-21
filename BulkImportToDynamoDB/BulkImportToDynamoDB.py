import boto3
import csv

dynamodb = boto3.resource('dynamodb','eu-west-1')

def batch_write(table_name,rows):
    table = dynamodb.Table(table_name)

    with table.batch_writer() as batch:
        for row in rows:
            batch.put_item(Item=row)
    return True

def read_csv(csv_file,list):
    rows = csv.DictReader(open(csv_file))

    for row in rows:
        
        list.append(row)

if __name__ == '__main__':

    table_name = 'unicamsensordata'
    file_name = '../BulkImportToDynamoDB/2020_03_01T00_00_00_2020_04_01T00_00_00.csv'
    items = []

    read_csv(file_name, items)
    status = batch_write(table_name,items)

    if (status):
        print('Data is saved')
    else:
        print('Error while inserting data')