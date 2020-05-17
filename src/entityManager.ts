import Symbols from './symbols';
import { v4 as uuid } from 'uuid';

const AWS = require('aws-sdk');
const documentClient = new AWS.DynamoDB.DocumentClient({
  endpoint: 'http://localhost:8000',
});

function getTable(target: object): string {
  let tables: string[] = Reflect.getMetadata(Symbols.table, target);
  if (!tables) {
    throw new Error(
      'The entity ' +
        target.constructor.name +
        ' should have a @table(<TABLE_NAME>) decorator.'
    );
  } else if (tables.length !== 1) {
    throw new Error(
      'The entity ' +
        target.constructor.name +
        ' should only have one @table(<TABLE_NAME>) decorator, but multiple were found.'
    );
  }
  return tables[0];
}

function getHashKeyProperty(target: object): string {
  let hashKeys: string[] = Reflect.getMetadata(Symbols.hashKey, target);
  if (!hashKeys) {
    throw new Error(
      'The entity ' +
        target.constructor.name +
        ' should have a property with a @hashKey() decorator.'
    );
  } else if (hashKeys.length !== 1) {
    throw new Error(
      'The entity ' +
        target.constructor.name +
        ' should only have one property with a @hashKey() decorator, but multiple were found.'
    );
  }
  return hashKeys[0];
}

function getVersionProperty(target: object): string {
  let versions: string[] = Reflect.getMetadata(Symbols.version, target);
  if (versions) {
    if (versions.length > 1) {
      throw new Error(
        'The entity ' +
          target.constructor.name +
          ' should only have one property with a @version() decorator, but multiple were found.'
      );
    }
    return versions[0];
  }
  return null;
}

export default class EntityManager {
  save(object: object): Promise<object> {
    return new Promise((resolve, reject) => {
      let table: string = getTable(object);
      let hashKeyProperty: string = getHashKeyProperty(object);
      let versionProperty: string = getVersionProperty(object);
      let params = {
        TableName: table,
        Item: {},
        Expected: {},
      };
      if (!object[hashKeyProperty]) {
        params.Expected[hashKeyProperty] = {
          Exists: false,
        };
      }
      params.Item[hashKeyProperty] = object[hashKeyProperty] || uuid();
      if (versionProperty) {
        if (object[versionProperty]) {
          params.Expected[versionProperty] = {
            ComparisonOperator: 'EQ',
            Value: object[versionProperty],
          };
        } else {
          params.Expected[versionProperty] = {
            Exists: false,
          };
        }
        params.Item[versionProperty] = (object[versionProperty] || 0) + 1;
      }
      documentClient.put(params, (err) => {
        if (err) reject(err);
        else {
          object[hashKeyProperty] = params.Item[hashKeyProperty];
          if (versionProperty) {
            object[versionProperty] = params.Item[versionProperty];
          }
          resolve(object);
        }
      });
    });
  }
}