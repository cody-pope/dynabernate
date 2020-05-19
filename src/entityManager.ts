import Symbols from './symbols';
import { v4 as uuid } from 'uuid';
import AWS = require('aws-sdk');

function getTable(target: any): string {
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

function getHashKeyProperty(target: any): string {
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

function getVersionProperty(target: any): string {
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

function getAttributeProperties(target: any): string[] {
  let attributes: string[] = Reflect.getMetadata(Symbols.attribute, target);
  return attributes;
}

export default class EntityManager {
  documentClient: AWS.DynamoDB.DocumentClient;

  constructor(documentClient: AWS.DynamoDB.DocumentClient) {
    this.documentClient = documentClient;
  }

  get<T>(example: T): Promise<T> {
    return new Promise((resolve, reject) => {
      const table = getTable(example);
      const hashKeyProperty = getHashKeyProperty(example);
      const versionProperty = getVersionProperty(example);
      const attributeProperties = getAttributeProperties(example);
      const params = {
        TableName: table,
        Key: {},
      };
      params.Key[hashKeyProperty] = example[hashKeyProperty];
      this.documentClient.get(params, (err, data) => {
        if (err) reject(err);
        else if (!data.Item) resolve();
        else {
          example[hashKeyProperty] = data.Item[hashKeyProperty];
          if (versionProperty) {
            example[versionProperty] = data.Item[versionProperty];
          }
          if (attributeProperties) {
            for (let attributeProperty of attributeProperties) {
              example[attributeProperty] = data.Item[attributeProperty];
            }
          }
          resolve(example);
        }
      });
    });
  }

  save<T>(object: T): Promise<T> {
    return new Promise((resolve, reject) => {
      const table = getTable(object);
      const hashKeyProperty = getHashKeyProperty(object);
      const versionProperty = getVersionProperty(object);
      const attributeProperties = getAttributeProperties(object);
      const params = {
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
      if (attributeProperties) {
        for (let attributeProperty of attributeProperties) {
          params.Item[attributeProperty] = object[attributeProperty];
        }
      }
      this.documentClient.put(params, (err) => {
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
