import 'reflect-metadata';
import Symbols from './symbols';

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

export function table(name: string): (constructor: Function) => void {
  return (constructor: Function) => {
    addStringToSymbol(Symbols.table, name, constructor.prototype);
  };
}

export function hashKey(): (target: object, propertyKey: string) => void {
  return (target: object, propertyKey: string) => {
    addStringToSymbol(Symbols.hashKey, propertyKey, target);
  };
}

export function attribute(): (target: object, propertyKey: string) => void {
  return (target: object, propertyKey: string) => {
    addStringToSymbol(Symbols.attribute, propertyKey, target);
  };
}

export function version(): (target: object, propertyKey: string) => void {
  return (target: object, propertyKey: string) => {
    addStringToSymbol(Symbols.version, propertyKey, target);
  };
}
