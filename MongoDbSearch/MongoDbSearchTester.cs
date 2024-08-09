using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.IdGenerators;
using MongoDB.Bson.Serialization.Serializers;
using MongoDB.Driver;
using MongoDB.Driver.Linq;
using MongoDbSearch.Models;

namespace MongoDbSearch
{
    internal class MongoDbSearchTester
    {
        private readonly string connectionString = "";
        private readonly string databaseName = "MongoDbSearch";
        private readonly string collectionName = "phoneBookContacts";

        private readonly MongoClientSettings settings;
        private readonly PhoneBookContact contact1;
        private readonly PhoneBookContact contact2;
        private readonly PhoneBookContact manager;
        private readonly PhoneBookContact secretary;
        private readonly List<PhoneBookContact> inMemoryCollection;

        internal MongoDbSearchTester()
        {
            if (string.IsNullOrEmpty(connectionString))
                throw new ArgumentException("connectionString is empty, please fill it out");
            ConfigureObjectMapping();
            settings = MongoClientSettings.FromConnectionString(connectionString);
            manager = new()
            {
                Id = ObjectId.GenerateNewId().ToString(),
                FirstName = "manager",
                LastName = "Meier",
                Location = "Züri",
                Numbers = [
                    new PhoneBookContactNumber { Id = ObjectId.GenerateNewId().ToString(), Number = "+41587770005", Type = NumberType.Office },
                ]
            };
            secretary = new()
            {
                Id = ObjectId.GenerateNewId().ToString(),
                FirstName = "secretary",
                LastName = "Müller",
                Location = "Züri",
                Numbers = [
                    new PhoneBookContactNumber { Id = ObjectId.GenerateNewId().ToString(), Number = "+41587770006", Type = NumberType.Office },
                ]
            };
            contact1 = new()
            {
                Id = ObjectId.GenerateNewId().ToString(),
                FirstName = "contact 1",
                LastName = "Grossmeister",
                Location = "Bern",
                Numbers = [
                        new PhoneBookContactNumber { Id = ObjectId.GenerateNewId().ToString(), Number = "+41587770001", Type = NumberType.Office },
                        new PhoneBookContactNumber { Id = ObjectId.GenerateNewId().ToString(), Number = "+41767770002", Type = NumberType.Mobile }
                    ], 
                ManagerId = manager.Id,
                SecretaryIds = [secretary.Id, manager.Id]
            };
            contact2 = new()
            {
                Id = ObjectId.GenerateNewId().ToString(),
                FirstName = "contact 2",
                LastName = "Meister",
                Location = "Bern",
                Numbers = [
                    new PhoneBookContactNumber { Id = ObjectId.GenerateNewId().ToString(), Number = "+41587770003", Type = NumberType.Office },
                    new PhoneBookContactNumber { Id = ObjectId.GenerateNewId().ToString(), Number = "+41767770004", Type = NumberType.Mobile }
                ]
            };
            inMemoryCollection = [contact1, contact2, manager, secretary];
        }

        internal void SearchTest()
        {
            List<IIdItem> rollbackObjects = [];
            MongoClient? client = null;
            IMongoCollection<PhoneBookContact>? col = null;
            try
            {
                client = new MongoClient(settings);
                var db = client.GetDatabase(databaseName);
                col = db.GetCollection<PhoneBookContact>(collectionName);

                //add test objects
                col.InsertOne(manager);
                rollbackObjects.Add(manager);
                col.InsertOne(secretary);
                rollbackObjects.Add(secretary);
                col.InsertOne(contact1);
                rollbackObjects.Add(contact1);
                col.InsertOne(contact2);
                rollbackObjects.Add(contact2);

                // get all PhoneBookContacts whose ManagerId property is not null. Should return contact1
                var managerIdEmptyFilter = Builders<PhoneBookContact>.Filter.Ne(nameof(PhoneBookContact.ManagerId), BsonNull.Value);

                var contactsWithEmptyManagerFromDb = col.AsQueryable().Where(x => managerIdEmptyFilter.Inject()).ToList();
                var actualContactsWithEmptyManager = inMemoryCollection.Where(u => u.ManagerId == null).ToList();
                if (contactsWithEmptyManagerFromDb.Count == actualContactsWithEmptyManager.Count)
                    Log($"Proper number of contacts with empty manager returned from database");
                else
                    Log($"Search returned the wrong number of contacts with an empty manager: nb returned {contactsWithEmptyManagerFromDb}, expected: {actualContactsWithEmptyManager.Count}");

                // get all PhoneBookContacts whose ManagerId property is null. Should return conctact2, manager, secretary
                var managerIdNotEmptyFilter = Builders<PhoneBookContact>.Filter.Eq(nameof(PhoneBookContact.ManagerId), BsonNull.Value);

                var contactsWithNonEmptyManagerFromDb = col.AsQueryable().Where(x => managerIdNotEmptyFilter.Inject()).ToList();
                var actualContactsWithNonEmptyManager = inMemoryCollection.Where(u => u.ManagerId != null).ToList();
                if (contactsWithNonEmptyManagerFromDb.Count == actualContactsWithNonEmptyManager.Count)
                    Log($"Proper number of contacts with non manager returned from database");
                else
                    Log($"Search returned the wrong number of contacts with a non empty manager: nb returned {contactsWithNonEmptyManagerFromDb}, expected: {actualContactsWithNonEmptyManager.Count}");
            }
            catch (Exception e)
            {
                Log($"Something went wrong: {e.Message}");
            }
            finally // cleanup, delete all objects that were created
            {
                if (rollbackObjects?.Count > 0)
                {
                    var idsToDelete = rollbackObjects.Select(x => x.Id).ToList();
                    col.DeleteMany(u => idsToDelete.Contains(u.Id));
                }
            }
        }

        internal void AggregationTest()
        {
            List<IIdItem> rollbackObjects = [];
            MongoClient? client = null;
            IMongoCollection<PhoneBookContact>? col = null;
            try
            {
                client = new MongoClient(settings);
                var db = client.GetDatabase(databaseName);
                col = db.GetCollection<PhoneBookContact>(collectionName);

                //add test objects
                col.InsertOne(manager);
                rollbackObjects.Add(manager);
                col.InsertOne(secretary);
                rollbackObjects.Add(secretary);
                col.InsertOne(contact1);
                rollbackObjects.Add(contact1);
                col.InsertOne(contact2);
                rollbackObjects.Add(contact2);

                var contact1FromDatabase = col.Find(u => u.Id == contact1.Id).FirstOrDefault();
                // => this is the contact from the database

                // now I'm filling the Manager and Secretary properties by looking up ManagerId and SecretaryIds.
                // testContacts is to the local code what col is to the MongoDb database, it contains the same 4 documents

                PhoneBookContact contact1WithLookup = contact1;
                contact1WithLookup.Manager = inMemoryCollection.FirstOrDefault(u => u.Id == contact1.ManagerId);
                contact1WithLookup.Secretary = [];

                foreach (var secretaryId in contact1.SecretaryIds) // look up all secretarys by their 'Id' property
                {
                    var secretary = inMemoryCollection.FirstOrDefault(u => u.Id == secretaryId);
                    if (secretary != null)
                        contact1WithLookup.Secretary.Add(secretary);
                }

                // and now I need the same from MongoDb

                var completeContact1FromDatabase = col
                    .Aggregate()
                    .Match(u => u.Id == contact1.Id)
                    .Lookup(collectionName, nameof(PhoneBookContact.ManagerId), "_id", nameof(PhoneBookContact.Manager))
                    .Unwind(nameof(PhoneBookContact.Manager), new AggregateUnwindOptions<PhoneBookContact> { PreserveNullAndEmptyArrays = true })
                    .Lookup(collectionName, nameof(PhoneBookContact.SecretaryIds), "_id", nameof(PhoneBookContact.Secretary))
                    .As<PhoneBookContact>()
                    .FirstOrDefault();

                if (completeContact1FromDatabase == null)
                {
                    Log($"Unable to get contact {contact1WithLookup.Id} from the database");
                    return;
                }
                if (completeContact1FromDatabase.Manager == null)
                {
                    Log($"Contact {contact1WithLookup.Id} has no manager but should have one");
                    return;
                }
                if (completeContact1FromDatabase.Secretary == null)
                {
                    Log($"Contact {contact1WithLookup.Id} has no secretaries but should have some");
                    return;
                }
                else
                {
                    var hasAllSecretaries = contact1WithLookup.Secretary.All(x => completeContact1FromDatabase.Secretary.Any(y => x.Id == y.Id));
                    if (!hasAllSecretaries)
                    {
                        Log($"Not all secretaries returned from database. Expected: {string.Join(",", contact1WithLookup.Secretary.Select(x => x.Id))}, received from database: {string.Join(",", completeContact1FromDatabase.Secretary.Select(x => x.Id))}");
                        return;
                    }
                }

                // and if we get here, we need to do this lookup again but on an IMongoQueryable

                var completeContact1FromDatabaseQueryable = col.AsQueryable().Where(x => x.Id == contact1.Id).First();
                // what do do here to get the Manager and Secretary filled
            }
            catch (Exception e)
            {
                Log($"Something went wrong: {e.Message}");
            }
            finally // cleanup, delete all objects that were created
            {
                if (rollbackObjects?.Count > 0)
                {
                    var idsToDelete = rollbackObjects.Select(x => x.Id).ToList();
                    col.DeleteMany(u => idsToDelete.Contains(u.Id));
                }
            }
        }

        private void ConfigureObjectMapping()
        {
            BsonClassMap.RegisterClassMap<PhoneBookContact>(cm =>
            {
                cm.AutoMap();
                cm.MapIdMember(c => c.Id)
                    .SetIdGenerator(StringObjectIdGenerator.Instance)
                    .SetSerializer(new StringSerializer(BsonType.ObjectId));
                cm.MapProperty(m => m.NumberOfTelephoneNumbers);
                cm.MapProperty(x => x.ManagerId).SetSerializer(new StringSerializer(BsonType.ObjectId));
                cm.MapMember(x => x.Numbers).SetSerializer(
                    new EnumerableInterfaceImplementerSerializer<List<PhoneBookContactNumber>, PhoneBookContactNumber>(
                        BsonSerializer.LookupSerializer<PhoneBookContactNumber>()));
                cm.MapProperty(x => x.SecretaryIds)
                    .SetSerializer(new EnumerableInterfaceImplementerSerializer<List<string>, string>(
                    new StringSerializer(BsonType.ObjectId)));
            });
        }

        private void Log(string message)
        {
            Console.WriteLine($"{DateTime.Now.ToString("HH:mm:ss")}|{message}");
        }

    }
}
