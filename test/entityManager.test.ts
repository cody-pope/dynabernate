import { expect, assert } from 'chai';
import { hashKey, attribute, version, table } from '../src/index';
import EntityManager from '../src/entityManager';
import { rejects } from 'assert';
import AWS = require('aws-sdk');
import DynamoDbLocal = require('dynamodb-local');
import { CreateTableInput } from 'aws-sdk/clients/dynamodb';

const tableName = 'TABLE_NAME';
const tableDefinition: CreateTableInput = {
  TableName: tableName,
  KeySchema: [
    {
      KeyType: 'HASH',
      AttributeName: 'id',
    },
  ],
  AttributeDefinitions: [
    {
      AttributeName: 'id',
      AttributeType: 'S',
    },
  ],
  BillingMode: 'PAY_PER_REQUEST',
};

const port = 8000;
const db = new AWS.DynamoDB({
  endpoint: 'http://localhost:' + port,
  region: 'us-east-1',
  accessKeyId: 'placeholder',
  secretAccessKey: 'placeholder',
});

function createTable(tableDefinition: CreateTableInput): Promise<unknown> {
  return new Promise((resolve, reject) => {
    db.createTable(tableDefinition, (err, data) => {
      if (err) reject(err);
      else resolve(data);
    });
  });
}

function scanTable(tableName: string): Promise<unknown> {
  return new Promise((resolve, reject) => {
    const params = {
      TableName: tableName,
    };
    db.scan(params, (err, data) => {
      if (err) reject(err);
      else resolve(data);
    });
  });
}

function deleteAllItems(tableName: string): Promise<unknown> {
  return scanTable(tableName).then((scanResult: object) => {
    let promise: Promise<unknown> = null;
    for (let item of scanResult['Items']) {
      let newPromise = new Promise((resolve, reject) => {
        const params = {
          TableName: tableName,
          Key: {
            id: {
              S: item['id']['S'],
            },
          },
        };
        db.deleteItem(params, (err, data) => {
          if (err) reject(err);
          else resolve(data);
        });
      });
      promise = promise ? promise.then(() => newPromise) : newPromise;
    }
    return promise;
  });
}

class TestEntityWithoutTable {}

@table(tableName)
@table(tableName)
class TestEntityWithMultipleTables {}

@table(tableName)
class TestEntityWithoutHashKey {}

@table(tableName)
class TestEntityWithMultipleHashKeysOnSameProperty {
  @hashKey()
  @hashKey()
  id: string;
}

@table(tableName)
class TestEntityWithMultipleHashKeysOnDifferentProperties {
  @hashKey()
  id: string;

  @hashKey()
  id2: string;
}

@table(tableName)
class TestEntityWithMultipleVersionsOnSameProperty {
  @hashKey()
  id: string;

  @version()
  @version()
  version: number;
}

@table(tableName)
class TestEntityWithMultipleVersionsOnDifferentProperties {
  @hashKey()
  id: string;

  @version()
  version: number;

  @version()
  version2: number;
}

@table(tableName)
class TestEntityWithTableAndHashKey {
  @hashKey()
  id: string;
}

@table(tableName)
class TestEntityWithTableAndHashKeyAndVersion {
  @hashKey()
  id: string;

  @version()
  version: number;
}

@table(tableName)
class TestEntityWithStringAttribute {
  @hashKey()
  id: string;

  @attribute()
  property: string;
}

@table(tableName)
class TestEntityWithNumberAttribute {
  @hashKey()
  id: string;

  @attribute()
  property: number;
}

@table(tableName)
class TestEntityWithBooleanAttribute {
  @hashKey()
  id: string;

  @attribute()
  property: boolean;
}

@table(tableName)
class TestEntityWithStringArrayAttribute {
  @hashKey()
  id: string;

  @attribute()
  property: string[];
}

@table(tableName)
class TestEntityWithObjectAttribute {
  @hashKey()
  id: string;

  @attribute()
  property: object;
}

@table(tableName)
class TestEntityWithTypedObjectAttribute {
  @hashKey()
  id: string;

  @attribute()
  property: TestEntityWithStringAttribute;
}

describe('EntityManager', () => {
  let entityManager: EntityManager;

  before(() => {
    return DynamoDbLocal.launch(port).then(() => {
      return createTable(tableDefinition);
    });
  });

  beforeEach(() => {
    entityManager = new EntityManager(
      new AWS.DynamoDB.DocumentClient({ service: db })
    );
  });

  afterEach(() => {
    return deleteAllItems(tableName);
  });

  after(() => {
    DynamoDbLocal.stop(port);
  });

  it('should save a new entity with a generated hash key when the hashKey property is null or undefined', async () => {
    const entity = new TestEntityWithTableAndHashKey();
    const persistedEntity = await entityManager.save(entity);
    expect(persistedEntity).to.be.equal(entity);
    const scanObject = await scanTable(tableName);
    expect(scanObject['Count']).to.be.equal(1);
    expect(scanObject['ScannedCount']).to.be.equal(1);
    expect(scanObject['Items'].length).to.be.equal(1);
    assert.match(
      scanObject['Items'][0]['id']['S'],
      /^[a-z0-9]{8}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{12}$/
    );
  });

  it('should save a transaction for a new entity with a generated hash key when the hashKey property is null or undefined', async () => {
    entityManager.beginWriteTransaction();
    const entity = new TestEntityWithTableAndHashKey();
    const persistedEntity = await entityManager.save(entity);
    expect(persistedEntity).to.be.equal(entity);
    await entityManager.commitWriteTransaction();
    const scanObject = await scanTable(tableName);
    expect(scanObject['Count']).to.be.equal(1);
    expect(scanObject['ScannedCount']).to.be.equal(1);
    expect(scanObject['Items'].length).to.be.equal(1);
    assert.match(
      scanObject['Items'][0]['id']['S'],
      /^[a-z0-9]{8}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{12}$/
    );
  });

  it('should save a new entity with version 1 when the version property is null or undefined', async () => {
    const entity = new TestEntityWithTableAndHashKeyAndVersion();
    const persistedEntity = await entityManager.save(entity);
    expect(persistedEntity).to.be.equal(entity);
    const scanObject = await scanTable(tableName);
    expect(scanObject['Count']).to.be.equal(1);
    expect(scanObject['ScannedCount']).to.be.equal(1);
    expect(scanObject['Items'].length).to.be.equal(1);
    expect(scanObject['Items'][0]['version']['N']).to.be.equal('1');
  });

  it('should save a transaction for a new entity with version 1 when the version property is null or undefined', async () => {
    entityManager.beginWriteTransaction();
    const entity = new TestEntityWithTableAndHashKeyAndVersion();
    const persistedEntity = await entityManager.save(entity);
    expect(persistedEntity).to.be.equal(entity);
    await entityManager.commitWriteTransaction();
    const scanObject = await scanTable(tableName);
    expect(scanObject['Count']).to.be.equal(1);
    expect(scanObject['ScannedCount']).to.be.equal(1);
    expect(scanObject['Items'].length).to.be.equal(1);
    expect(scanObject['Items'][0]['version']['N']).to.be.equal('1');
  });

  it('should save an existing entity without generating hash key', async () => {
    const entity = new TestEntityWithTableAndHashKey();
    entity.id = '1234';
    const persistedEntity = await entityManager.save(entity);
    expect(persistedEntity).to.be.equal(entity);
    const scanObject = await scanTable(tableName);
    expect(scanObject).to.deep.equal({
      Count: 1,
      ScannedCount: 1,
      Items: [
        {
          id: {
            S: '1234',
          },
        },
      ],
    });
  });

  it('should save an entity with a string attribute', async () => {
    const entity = new TestEntityWithStringAttribute();
    entity.id = '1234';
    entity.property = 'TEST_VALUE';
    const persistedEntity = await entityManager.save(entity);
    expect(persistedEntity).to.be.equal(entity);
    const scanObject = await scanTable(tableName);
    expect(scanObject).to.deep.equal({
      Count: 1,
      ScannedCount: 1,
      Items: [
        {
          id: {
            S: '1234',
          },
          property: {
            S: 'TEST_VALUE',
          },
        },
      ],
    });
  });

  it('should save an entity with a number attribute', async () => {
    const entity = new TestEntityWithNumberAttribute();
    entity.id = '1234';
    entity.property = 8378;
    const persistedEntity = await entityManager.save(entity);
    expect(persistedEntity).to.be.equal(entity);
    const scanObject = await scanTable(tableName);
    expect(scanObject).to.deep.equal({
      Count: 1,
      ScannedCount: 1,
      Items: [
        {
          id: {
            S: '1234',
          },
          property: {
            N: '8378',
          },
        },
      ],
    });
  });

  it('should save an entity with a boolean attribute', async () => {
    const entity = new TestEntityWithBooleanAttribute();
    entity.id = '1234';
    entity.property = true;
    const persistedEntity = await entityManager.save(entity);
    expect(persistedEntity).to.be.equal(entity);
    const scanObject = await scanTable(tableName);
    expect(scanObject).to.deep.equal({
      Count: 1,
      ScannedCount: 1,
      Items: [
        {
          id: {
            S: '1234',
          },
          property: {
            BOOL: true,
          },
        },
      ],
    });
  });

  it('should save an entity with a null attribute', async () => {
    const entity = new TestEntityWithStringAttribute();
    entity.id = '1234';
    entity.property = null;
    const persistedEntity = await entityManager.save(entity);
    expect(persistedEntity).to.be.equal(entity);
    const scanObject = await scanTable(tableName);
    expect(scanObject).to.deep.equal({
      Count: 1,
      ScannedCount: 1,
      Items: [
        {
          id: {
            S: '1234',
          },
          property: {
            NULL: true,
          },
        },
      ],
    });
  });

  it('should save an entity with a string array attribute', async () => {
    const entity = new TestEntityWithStringArrayAttribute();
    entity.id = '1234';
    entity.property = ['TEST_VALUE'];
    const persistedEntity = await entityManager.save(entity);
    expect(persistedEntity).to.be.equal(entity);
    const scanObject = await scanTable(tableName);
    expect(scanObject).to.deep.equal({
      Count: 1,
      ScannedCount: 1,
      Items: [
        {
          id: {
            S: '1234',
          },
          property: {
            L: [
              {
                S: 'TEST_VALUE',
              },
            ],
          },
        },
      ],
    });
  });

  it('should save an entity with an object attribute', async () => {
    const entity = new TestEntityWithObjectAttribute();
    entity.id = '1234';
    entity.property = {
      TEST_KEY: 'TEST_VALUE',
    };
    const persistedEntity = await entityManager.save(entity);
    expect(persistedEntity).to.be.equal(entity);
    const scanObject = await scanTable(tableName);
    expect(scanObject).to.deep.equal({
      Count: 1,
      ScannedCount: 1,
      Items: [
        {
          id: {
            S: '1234',
          },
          property: {
            M: {
              TEST_KEY: {
                S: 'TEST_VALUE',
              },
            },
          },
        },
      ],
    });
  });

  it('should save an entity with an object attribute with nested objects', async () => {
    const entity = new TestEntityWithObjectAttribute();
    entity.id = '1234';
    entity.property = {
      TEST_KEY: {
        TEST_KEY_2: 'TEST_VALUE',
      },
    };
    const persistedEntity = await entityManager.save(entity);
    expect(persistedEntity).to.be.equal(entity);
    const scanObject = await scanTable(tableName);
    expect(scanObject).to.deep.equal({
      Count: 1,
      ScannedCount: 1,
      Items: [
        {
          id: {
            S: '1234',
          },
          property: {
            M: {
              TEST_KEY: {
                M: {
                  TEST_KEY_2: {
                    S: 'TEST_VALUE',
                  },
                },
              },
            },
          },
        },
      ],
    });
  });

  it('should save an entity with a typed object attribute', async () => {
    const entity = new TestEntityWithTypedObjectAttribute();
    entity.id = '1234';
    entity.property = new TestEntityWithStringAttribute();
    entity.property.id = '5678';
    entity.property.property = 'TEST_VALUE';
    const persistedEntity = await entityManager.save(entity);
    expect(persistedEntity).to.be.equal(entity);
    const scanObject = await scanTable(tableName);
    expect(scanObject).to.deep.equal({
      Count: 1,
      ScannedCount: 1,
      Items: [
        {
          id: {
            S: '1234',
          },
          property: {
            M: {
              id: {
                S: '5678',
              },
              property: {
                S: 'TEST_VALUE',
              },
            },
          },
        },
      ],
    });
  });

  it('should save an existing entity with incremented version', async () => {
    const entity = await entityManager.save(
      new TestEntityWithTableAndHashKeyAndVersion()
    );
    expect(entity.version).to.be.equal(1);
    const persistedEntity = await entityManager.save(entity);
    expect(persistedEntity.version).to.be.equal(2);
    const scanObject = await scanTable(tableName);
    expect(scanObject['Count']).to.be.equal(1);
    expect(scanObject['ScannedCount']).to.be.equal(1);
    expect(scanObject['Items'].length).to.be.equal(1);
    expect(scanObject['Items'][0]['version']['N']).to.be.equal('2');
  });

  it('should not save an existing entity without having read previous version first', async () => {
    const entity = await entityManager.save(
      new TestEntityWithTableAndHashKeyAndVersion()
    );
    expect(entity.version).to.be.equal(1);
    entity.version = null;
    await rejects(entityManager.save(entity), {
      name: 'ConditionalCheckFailedException',
      message: 'The conditional request failed',
    });
    const scanObject = await scanTable(tableName);
    expect(scanObject['Count']).to.be.equal(1);
    expect(scanObject['ScannedCount']).to.be.equal(1);
    expect(scanObject['Items'].length).to.be.equal(1);
    expect(scanObject['Items'][0]['version']['N']).to.be.equal('1');
  });

  it('should not save an existing entity when trying to use wrong version', async () => {
    const entity = await entityManager.save(
      new TestEntityWithTableAndHashKeyAndVersion()
    );
    expect(entity.version).to.be.equal(1);
    entity.version = 2;
    await rejects(entityManager.save(entity), {
      name: 'ConditionalCheckFailedException',
      message: 'The conditional request failed',
    });
    const scanObject = await scanTable(tableName);
    expect(scanObject['Count']).to.be.equal(1);
    expect(scanObject['ScannedCount']).to.be.equal(1);
    expect(scanObject['Items'].length).to.be.equal(1);
    expect(scanObject['Items'][0]['version']['N']).to.be.equal('1');
  });

  it('should get an existing entity', async () => {
    const persistedEntity = await entityManager.save(
      new TestEntityWithTableAndHashKeyAndVersion()
    );
    const example = new TestEntityWithTableAndHashKeyAndVersion();
    example.id = persistedEntity.id;
    const entity = await entityManager.get(example);
    expect(entity).to.deep.equal(persistedEntity);
    const scanObject = await scanTable(tableName);
    expect(scanObject['Count']).to.be.equal(1);
  });

  it('should get an existing entity with typed object attribute', async () => {
    const entityToSave = new TestEntityWithTypedObjectAttribute();
    entityToSave.id = '1234';
    entityToSave.property = new TestEntityWithStringAttribute();
    entityToSave.property.id = '5678';
    entityToSave.property.property = 'TEST_VALUE';
    const persistedEntity = await entityManager.save(entityToSave);
    const example = new TestEntityWithTypedObjectAttribute();
    example.id = persistedEntity.id;
    const entity = await entityManager.get(example);
    expect(entity).to.deep.equal(persistedEntity);
    const scanObject = await scanTable(tableName);
    expect(scanObject['Count']).to.be.equal(1);
  });

  it('should get an existing entity that does not have a version', async () => {
    const persistedEntity = await entityManager.save(
      new TestEntityWithTableAndHashKey()
    );
    const example = new TestEntityWithTableAndHashKey();
    example.id = persistedEntity.id;
    const entity = await entityManager.get(example);
    expect(entity).to.deep.equal(persistedEntity);
    const scanObject = await scanTable(tableName);
    expect(scanObject['Count']).to.be.equal(1);
  });

  it('should not get an entity that does not exist', async () => {
    const example = new TestEntityWithTableAndHashKeyAndVersion();
    example.id = '1234';
    const entity = await entityManager.get(example);
    expect(entity).to.be.undefined;
    const scanObject = await scanTable(tableName);
    expect(scanObject['Count']).to.be.equal(0);
  });

  it('should not get an entity without an id', async () => {
    await rejects(
      entityManager.get(new TestEntityWithTableAndHashKeyAndVersion()),
      {
        name: 'ValidationException',
        message: 'The number of conditions on the keys is invalid',
      }
    );
    const scanObject = await scanTable(tableName);
    expect(scanObject['Count']).to.be.equal(0);
  });

  it('should delete an existing entity', async () => {
    const entity = await entityManager.save(
      new TestEntityWithTableAndHashKeyAndVersion()
    );
    expect(entity.version).to.be.equal(1);
    let scanObject = await scanTable(tableName);
    expect(scanObject['Count']).to.be.equal(1);
    expect(scanObject['ScannedCount']).to.be.equal(1);
    await entityManager.delete(entity);
    scanObject = await scanTable(tableName);
    expect(scanObject['Count']).to.be.equal(0);
    expect(scanObject['ScannedCount']).to.be.equal(0);
  });

  it('should delete an existing entity in a transaction', async () => {
    const entity = await entityManager.save(
      new TestEntityWithTableAndHashKeyAndVersion()
    );
    expect(entity.version).to.be.equal(1);
    let scanObject = await scanTable(tableName);
    expect(scanObject['Count']).to.be.equal(1);
    expect(scanObject['ScannedCount']).to.be.equal(1);
    entityManager.beginWriteTransaction();
    await entityManager.delete(entity);
    await entityManager.commitWriteTransaction();
    scanObject = await scanTable(tableName);
    expect(scanObject['Count']).to.be.equal(0);
    expect(scanObject['ScannedCount']).to.be.equal(0);
  });

  it('should delete an existing entity that has already been deleted', async () => {
    const entity = await entityManager.save(
      new TestEntityWithTableAndHashKeyAndVersion()
    );
    expect(entity.version).to.be.equal(1);
    let scanObject = await scanTable(tableName);
    expect(scanObject['Count']).to.be.equal(1);
    expect(scanObject['ScannedCount']).to.be.equal(1);
    await entityManager.delete(entity);
    scanObject = await scanTable(tableName);
    expect(scanObject['Count']).to.be.equal(0);
    expect(scanObject['ScannedCount']).to.be.equal(0);
    await entityManager.delete(entity);
    scanObject = await scanTable(tableName);
    expect(scanObject['Count']).to.be.equal(0);
    expect(scanObject['ScannedCount']).to.be.equal(0);
  });

  it('should not delete an entity when id has not been provided', async () => {
    await rejects(
      entityManager.delete(new TestEntityWithTableAndHashKeyAndVersion()),
      {
        name: 'ValidationException',
        message: 'The number of conditions on the keys is invalid',
      }
    );
  });

  it('should commit a transaction with nothing in it', async () => {
    entityManager.beginWriteTransaction();
    await entityManager.commitWriteTransaction();
  });

  it('should not commit a transaction with bad version', async () => {
    let entity1 = new TestEntityWithTableAndHashKeyAndVersion();
    entity1.id = 'ENTITY_1';
    entity1 = await entityManager.save(entity1);
    entityManager.beginWriteTransaction();
    let entity2 = new TestEntityWithTableAndHashKeyAndVersion();
    entity2.id = 'ENTITY_1';
    entity2 = await entityManager.save(entity2);
    let entity3 = new TestEntityWithTableAndHashKeyAndVersion();
    entity3.id = 'ENTITY_3';
    entity3 = await entityManager.save(entity3);
    await rejects(entityManager.commitWriteTransaction(), {
      name: 'TransactionCanceledException',
      message:
        'Transaction cancelled, please refer cancellation reasons for specific reasons [ConditionalCheckFailed, None]',
    });

    const scanObject = await scanTable(tableName);
    expect(scanObject['Count']).to.be.equal(1);
    expect(scanObject['ScannedCount']).to.be.equal(1);
    expect(scanObject['Items'].length).to.be.equal(1);
    expect(scanObject['Items'][0]['version']['N']).to.be.equal('1');
  });
});
