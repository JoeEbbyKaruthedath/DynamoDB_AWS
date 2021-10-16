import boto3

#Quering Data using the Primary and sort key
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
table = dynamodb.Table('Music')

resp = table.get_item(Key={"title": "Redemption", "artist": "Bob Marley"})
print("****************************Quering Data************************************")
print(resp['Item'])

#Quering data using Primary Key/ Using Song Title
import boto3
from boto3.dynamodb.conditions import Key

# boto3 is the AWS SDK library for Python.
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
table = dynamodb.Table('Music')

# When making a Query API call, we use the KeyConditionExpression parameter to specify the hash key on which we want to query.
# We're using the Key object from the Boto3 library to specify that we want the attribute name ("Author")
# to equal "John Grisham" by using the ".eq()" method.
resp = table.query(KeyConditionExpression=Key('title').eq('Puthiyamugham'))

print("\n**************Quering Data with title of the song:**************************")
for item in resp['Items']:
    print(item)


#Creating a global secondary index to query using the name of the artist
client = boto3.client('dynamodb', region_name='us-east-1')

try:
    resp = client.update_table(
        TableName="Music",
        # Any attributes used in your new global secondary index must be declared in AttributeDefinitions
        AttributeDefinitions=[
            {
                "AttributeName": "artist",
                "AttributeType": "S"
            },
        ],
        # This is where you add, update, or delete any global secondary indexes on your table.
        GlobalSecondaryIndexUpdates=[
            {
                "Create": {
                    # You need to name your index and specifically refer to it when using it for queries.
                    "IndexName": "ArtistIndex",
                    # Like the table itself, you need to specify the key schema for an index.
                    # For a global secondary index, you can use a simple or composite key schema.
                    "KeySchema": [
                        {
                            "AttributeName": "artist",
                            "KeyType": "HASH"
                        }
                    ],
                    # You can choose to copy only specific attributes from the original item into the index.
                    # You might want to copy only a few attributes to save space.
                    "Projection": {
                        "ProjectionType": "ALL"
                    },
                    # Global secondary indexes have read and write capacity separate from the underlying table.
                    "ProvisionedThroughput": {
                        "ReadCapacityUnits": 1,
                        "WriteCapacityUnits": 1,
                    }
                }
            }
        ],
    )
    print(" \n Secondary index added!")
except Exception as e:
    print("Error updating table:")
    print(e)

#Quering withthe name of the artist, which is now set as the global secondary index
import time
from boto3.dynamodb.conditions import Key


dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
table = dynamodb.Table('Music')

# When adding a global secondary index to an existing table, you cannot query the index until it has been backfilled.
# This portion of the script waits until the index is in the “ACTIVE” status, indicating it is ready to be queried.
while True:
    if not table.global_secondary_indexes or table.global_secondary_indexes[0]['IndexStatus'] != 'ACTIVE':
        print('Waiting for index to backfill...')
        time.sleep(5)
        table.reload()
    else:
        break

# When making a Query call, you use the KeyConditionExpression parameter to specify the hash key on which you want to query.
# If you want to use a specific index, you also need to pass the IndexName in our API call.
resp = table.query(
    # Add the name of the index you want to use in your query.
    IndexName="ArtistIndex",
    KeyConditionExpression=Key('artist').eq('Bob Dylan'),
)

print(**"\n Quering using the name of the artist which is now the Global Secondary Index:"**)
for item in resp['Items']:
    print(item)
