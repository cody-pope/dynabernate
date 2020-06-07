import { expect } from 'chai';
import EntityMeta from '../src/entityMeta';

describe('EntityMeta', () => {
  it('should get table name when entity has exactly one table name associated to it', () => {
    const tableName = 'TABLE_NAME';
    const entity = {};
    EntityMeta.addTableName(entity, tableName);
    expect(EntityMeta.getTableName(entity)).to.be.equal(tableName);
  });

  it('should throw error when entity does not have table name associated to it', () => {
    const entity = {};
    expect(() => EntityMeta.getTableName(entity)).to.throw(
      Error,
      'The entity Object should have a @table(<TABLE_NAME>) decorator.'
    );
  });

  it('should throw error when entity has multiple table names associated to it', () => {
    const entity = {};
    EntityMeta.addTableName(entity, 'TABLE_NAME_1');
    EntityMeta.addTableName(entity, 'TABLE_NAME_2');
    expect(() => EntityMeta.getTableName(entity)).to.throw(
      Error,
      'The entity Object should only have one @table(<TABLE_NAME>) decorator, but multiple were found.'
    );
  });

  it('should get hash key name when entity has exactly one hash key name associated to it', () => {
    const hashKeyName = 'ID';
    const entity = {};
    EntityMeta.addHashKeyName(entity, hashKeyName);
    expect(EntityMeta.getHashKeyName(entity)).to.be.equal(hashKeyName);
  });

  it('should throw error when entity does not have hash key name associated to it', () => {
    const entity = {};
    expect(() => EntityMeta.getHashKeyName(entity)).to.throw(
      Error,
      'The entity Object should have a property with a @hashKey() decorator.'
    );
  });

  it('should throw error when entity has multiple hash key names associated to it', () => {
    const hashKeyName = 'ID';
    const entity = {};
    EntityMeta.addHashKeyName(entity, hashKeyName);
    EntityMeta.addHashKeyName(entity, hashKeyName);
    expect(() => EntityMeta.getHashKeyName(entity)).to.throw(
      Error,
      'The entity Object should only have one property with a @hashKey() decorator, but multiple were found.'
    );
  });

  it('should get version name when entity has exactly one version associated to it', () => {
    const versionName = 'version';
    const entity = {};
    EntityMeta.addVersionName(entity, versionName);
    expect(EntityMeta.getVersionName(entity)).to.be.equal(versionName);
  });

  it('should get null version name when entity has no version associated to it', () => {
    const entity = {};
    expect(EntityMeta.getVersionName(entity)).to.be.null;
  });

  it('should throw error when entity has multiple versions associated to it', () => {
    const versionName = 'version';
    const entity = {};
    EntityMeta.addVersionName(entity, versionName);
    EntityMeta.addVersionName(entity, versionName);
    expect(() => EntityMeta.getVersionName(entity)).to.throw(
      Error,
      'The entity Object should only have one property with a @version() decorator, but multiple were found.'
    );
  });

  it('should get empty array when entity has no attributes associated to it', () => {
    const entity = {};
    expect(EntityMeta.getAttributeNames(entity)).to.deep.equal([]);
  });

  it('should get attribute when entity has one attribute associated to it', () => {
    const attributeName = 'attributeName';
    const entity = {};
    EntityMeta.addAttributeName(entity, attributeName);
    expect(EntityMeta.getAttributeNames(entity)).to.deep.equal([attributeName]);
  });

  it('should get attributes when entity has two attributes associated to it', () => {
    const attributeName = 'attributeName';
    const attributeName2 = 'attributeName2';
    const entity = {};
    EntityMeta.addAttributeName(entity, attributeName);
    EntityMeta.addAttributeName(entity, attributeName2);
    expect(EntityMeta.getAttributeNames(entity)).to.deep.equal([
      attributeName,
      attributeName2,
    ]);
  });
});
