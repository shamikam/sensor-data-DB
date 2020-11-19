from decimal import Decimal
import json
import boto3


def load_sensor_data(sensor_data, dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb', region_name='eu-west-2')

    table = dynamodb.Table('Sensor_data')
    for data_row in sensor_data:
        attr_time = data_row['attr_time']
        id = data_row['id']
        print("Adding sensor:", id, attr_time)
        table.put_item(Item=data_row)


if __name__ == '__main__':
    with open("acc_climbingdown_chest.json") as json_file:
        sensor_list = json.load(json_file, parse_float=Decimal)
    load_sensor_data(sensor_list)

