import 'reflect-metadata';

function addStringToSymbol(
  symbol: Symbol,
  string: string,
  target: object
): void {
  let strings: string[] = Reflect.getMetadata(symbol, target);
  if (!strings) {
    strings = [];
  }
  strings.push(string);
  Reflect.defineMetadata(symbol, strings, target);
}

export default class EntityMeta {
  private static table: Symbol = Symbol('table');
  private static hashKey: Symbol = Symbol('hashKey');
  private static attribute: Symbol = Symbol('attribute');
  private static version: Symbol = Symbol('version');

  static getTableName(target: any): string {
    let tables: string[] = Reflect.getMetadata(this.table, target);
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

  static getHashKeyName(target: any): string {
    let hashKeys: string[] = Reflect.getMetadata(this.hashKey, target);
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

  static getVersionName(target: any): string {
    let versions: string[] = Reflect.getMetadata(this.version, target);
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

  static getAttributeNames(target: any): string[] {
    let attributes: string[] = Reflect.getMetadata(this.attribute, target);
    return attributes || [];
  }

  static addTableName(target: any, name: string): void {
    addStringToSymbol(this.table, name, target);
  }

  static addHashKeyName(target: any, name: string): void {
    addStringToSymbol(this.hashKey, name, target);
  }

  static addVersionName(target: any, name: string): void {
    addStringToSymbol(this.version, name, target);
  }

  static addAttributeName(target: any, name: string): void {
    addStringToSymbol(this.attribute, name, target);
  }
}
