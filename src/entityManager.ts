import EntityMeta from './entityMeta';
import { v4 as uuid } from 'uuid';
import {
  DocumentClient,
  TransactWriteItemsInput,
  Put,
} from 'aws-sdk/clients/dynamodb';

export default class EntityManager {
  private documentClient: DocumentClient;
  private writeTxn: TransactWriteItemsInput;

  constructor(documentClient: DocumentClient) {
    this.documentClient = documentClient;
  }

  beginWriteTransaction(): void {
    this.writeTxn = {
      TransactItems: [],
    };
  }

  commitWriteTransaction(): Promise<void> {
    return new Promise((resolve, reject) => {
      if (!this.writeTxn || this.writeTxn.TransactItems.length === 0) {
        this.writeTxn = undefined;
        resolve();
      } else {
        this.documentClient.transactWrite(this.writeTxn, (err) => {
          if (err) reject(err);
          else {
            this.writeTxn = undefined;
            resolve();
          }
        });
      }
    });
  }

  delete<T>(example: T): Promise<T> {
    return new Promise((resolve, reject) => {
      const tableName = EntityMeta.getTableName(example);
      const hashKeyName = EntityMeta.getHashKeyName(example);
      const params = {
        TableName: tableName,
        Key: {},
      };
      params.Key[hashKeyName] = example[hashKeyName];
      if (this.writeTxn) {
        this.writeTxn.TransactItems.push({
          Delete: params,
        });
        resolve();
      } else {
        this.documentClient.delete(params, (err) => {
          if (err) reject(err);
          else resolve();
        });
      }
    });
  }

  get<T>(example: T): Promise<T> {
    return new Promise((resolve, reject) => {
      const tableName = EntityMeta.getTableName(example);
      const hashKeyName = EntityMeta.getHashKeyName(example);
      const versionName = EntityMeta.getVersionName(example);
      const attributeNames = EntityMeta.getAttributeNames(example);
      const params = {
        TableName: tableName,
        Key: {},
      };
      params.Key[hashKeyName] = example[hashKeyName];
      this.documentClient.get(params, (err, data) => {
        if (err) reject(err);
        else if (!data.Item) resolve();
        else {
          example[hashKeyName] = data.Item[hashKeyName];
          if (versionName) {
            example[versionName] = data.Item[versionName];
          }
          for (let attributeProperty of attributeNames) {
            example[attributeProperty] = data.Item[attributeProperty];
          }
          resolve(example);
        }
      });
    });
  }

  save<T>(entity: T): Promise<T> {
    return new Promise((resolve, reject) => {
      const params: Put = {
        TableName: EntityMeta.getTableName(entity),
        Item: {},
        ExpressionAttributeValues: {},
      };

      const conditions = [];

      // Add hash key
      const hashKeyName = EntityMeta.getHashKeyName(entity);
      if (!entity[hashKeyName]) {
        conditions.push('attribute_not_exists(' + hashKeyName + ')');
      }
      params.Item[hashKeyName] = entity[hashKeyName] || uuid();

      // Add version
      const versionName = EntityMeta.getVersionName(entity);
      if (versionName) {
        if (entity[versionName]) {
          conditions.push(versionName + ' = :version');
          params.ExpressionAttributeValues[':version'] = entity[versionName];
        } else {
          conditions.push('attribute_not_exists(' + versionName + ')');
        }
        params.Item[versionName] = (entity[versionName] || 0) + 1;
      }

      // Add attributes
      const attributeNames = EntityMeta.getAttributeNames(entity);
      for (let attributeProperty of attributeNames) {
        params.Item[attributeProperty] = entity[attributeProperty];
      }

      // Build conditional expression
      for (let condition of conditions) {
        if (params.ConditionExpression) {
          params.ConditionExpression += ' and ' + condition;
        } else {
          params.ConditionExpression = condition;
        }
      }

      // Remove expression attribute values if empty
      if (Object.keys(params.ExpressionAttributeValues).length === 0) {
        delete params.ExpressionAttributeValues;
      }

      if (this.writeTxn) {
        this.writeTxn.TransactItems.push({
          Put: params,
        });
        entity[hashKeyName] = params.Item[hashKeyName];
        if (versionName) {
          entity[versionName] = params.Item[versionName];
        }
        resolve(entity);
      } else {
        this.documentClient.put(params, (err) => {
          if (err) reject(err);
          else {
            entity[hashKeyName] = params.Item[hashKeyName];
            if (versionName) {
              entity[versionName] = params.Item[versionName];
            }
            resolve(entity);
          }
        });
      }
    });
  }
}
