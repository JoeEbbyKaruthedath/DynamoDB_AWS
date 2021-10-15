import boto3

# boto3 is the AWS SDK library for Python.
# We can use the low-level client to make API calls to DynamoDB.
client = boto3.client('dynamodb', region_name='us-east-1')

try:
    resp = client.create_table(
        TableName="Music",
        # Declare your Primary Key in the KeySchema argument
        KeySchema=[
            {
                "AttributeName": "title",
                "KeyType": "HASH"
            },
            {
                "AttributeName": "artist",
                "KeyType": "RANGE"
            }
        ],
        # Any attributes used in KeySchema or Indexes must be declared in AttributeDefinitions
        AttributeDefinitions=[
            {
                "AttributeName": "title",
                "AttributeType": "S"
            },
            {
                "AttributeName": "artist",
                "AttributeType": "S"
            }
        ],
        # ProvisionedThroughput controls the amount of data you can read or write to DynamoDB per second.
        # You can control read and write capacity independently.
        ProvisionedThroughput={
            "ReadCapacityUnits": 1,
            "WriteCapacityUnits": 1
        }
    )
    print("Table created successfully!")
except Exception as e:
    print("Error creating table:")
    print(e)
    
 #Adding a delay  
waiter = client.get_waiter("table_exists")
waiter.wait(TableName = "Music")


#Inserting Data
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
table = dynamodb.Table('Music')

with table.batch_writer() as batch:
    batch.put_item(Item={"title": "4 the people", "artist": "Jassie Gift",
        "album": "Lajjavathiye Ninte", "genre": "Jazz", "duration": "10:55" })
    batch.put_item(Item={"title": "Puthiyamugham", "artist": "Jassie Gift",
        "album": "Thattum Muttum", "genre":"Pop", "duration" : "5:55" })
    batch.put_item(Item={"title": "Highway 61", "artist": "Bob Dylan",
        "album": "Like a rolling stone", "genre": "Classic" , "duration":"3:40" })
    batch.put_item(Item={"title": "Freewheelin", "artist": "Bob Dylan",
        "album": "Blowin in the wind", "genre": "Classic", "duration": "4:55" })
    batch.put_item(Item={"title": "Redemption", "artist": "Bob Marley",
        "album": "Redemption Song", "genre": "Pop", "duration": "7:21"  })
        
print("Data Inserted")