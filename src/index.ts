import EntityMeta from './entityMeta';

export function table(name: string): (constructor: Function) => void {
  return (constructor: Function) => {
    EntityMeta.addTableName(constructor.prototype, name);
  };
}

export function hashKey(): (target: object, propertyKey: string) => void {
  return (target: object, propertyKey: string) => {
    EntityMeta.addHashKeyName(target, propertyKey);
  };
}

export function attribute(): (target: object, propertyKey: string) => void {
  return (target: object, propertyKey: string) => {
    EntityMeta.addAttributeName(target, propertyKey);
  };
}

export function version(): (target: object, propertyKey: string) => void {
  return (target: object, propertyKey: string) => {
    EntityMeta.addVersionName(target, propertyKey);
  };
}
