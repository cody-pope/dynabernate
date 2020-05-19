import { expect, assert } from 'chai';
import { hashKey, attribute, version, table } from '../src/index';
import EntityManager from '../src/entityManager';
import { rejects } from 'assert';
import AWS = require('aws-sdk');
import DynamoDbLocal = require('dynamodb-local');
import { CreateTableInput } from 'aws-sdk/clients/dynamodb';

const tableName = 'TABLE_NAME';
// const tableDefinition = new CreateTableInput
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
// AWS.config.update({ region: 'us-east-1' });
const db = new AWS.DynamoDB({
  endpoint: 'http://localhost:' + port,
  region: 'us-east-1',
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

describe('EntityManager', () => {
  let entityManager: EntityManager;

  before(() => {
    return DynamoDbLocal.launch(port).then(() => {
      return createTable(tableDefinition);
    });
  });

  beforeEach(() => {
    entityManager = new EntityManager();
  });

  afterEach(() => {
    return deleteAllItems(tableName);
  });

  after(() => {
    DynamoDbLocal.stop(port);
  });

  it('should not save when entity does not have a table decorator', async () => {
    await rejects(entityManager.save(new TestEntityWithoutTable()), {
      name: 'Error',
      message:
        'The entity TestEntityWithoutTable should have a @table(<TABLE_NAME>) decorator.',
    });
  });

  it('should not save when entity has multiple table decorators', async () => {
    await rejects(entityManager.save(new TestEntityWithMultipleTables()), {
      name: 'Error',
      message:
        'The entity TestEntityWithMultipleTables should only have one @table(<TABLE_NAME>) decorator, but multiple were found.',
    });
  });

  it('should not save when entity does not have a property with a hashKey decorator', async () => {
    await rejects(entityManager.save(new TestEntityWithoutHashKey()), {
      name: 'Error',
      message:
        'The entity TestEntityWithoutHashKey should have a property with a @hashKey() decorator.',
    });
  });

  it('should not save when entity has multiple hashKey decorators on the same property', async () => {
    await rejects(
      entityManager.save(new TestEntityWithMultipleHashKeysOnSameProperty()),
      {
        name: 'Error',
        message:
          'The entity TestEntityWithMultipleHashKeysOnSameProperty should only have one property with a @hashKey() decorator, but multiple were found.',
      }
    );
  });

  it('should not save when entity has multiple hashKey decorators on the different properties', async () => {
    await rejects(
      entityManager.save(
        new TestEntityWithMultipleHashKeysOnDifferentProperties()
      ),
      {
        name: 'Error',
        message:
          'The entity TestEntityWithMultipleHashKeysOnDifferentProperties should only have one property with a @hashKey() decorator, but multiple were found.',
      }
    );
  });

  it('should not save when entity has multiple version decorators on the same property', async () => {
    await rejects(
      entityManager.save(new TestEntityWithMultipleVersionsOnSameProperty()),
      {
        name: 'Error',
        message:
          'The entity TestEntityWithMultipleVersionsOnSameProperty should only have one property with a @version() decorator, but multiple were found.',
      }
    );
  });

  it('should not save when entity has multiple version decorators on the different properties', async () => {
    await rejects(
      entityManager.save(
        new TestEntityWithMultipleVersionsOnDifferentProperties()
      ),
      {
        name: 'Error',
        message:
          'The entity TestEntityWithMultipleVersionsOnDifferentProperties should only have one property with a @version() decorator, but multiple were found.',
      }
    );
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
});
