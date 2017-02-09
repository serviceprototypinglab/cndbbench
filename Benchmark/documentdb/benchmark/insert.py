import pymongo

uri = "mongodb://shared-documentdb-mongo:f6avXcnsNxCizdXY4ptsaxE0cbOUf2g0TdEPX1SRi16l072IXpsBCiusUw93WO5CN2PBIMd" \
      "TriEQ6FjvsHOFYg==@shared-documentdb-mongo.documents.azure.com:10250/?ssl=true&ssl_cert_reqs=CERT_NONE"
client = pymongo.MongoClient(uri)
db = client.example
db.blobstore.insert_one({"a": 1})
print("end")
